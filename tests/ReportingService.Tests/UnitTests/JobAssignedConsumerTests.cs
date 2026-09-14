using Confluent.Kafka;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

using ReportingService.Messaging.Consumers;
using ReportingService.Messaging.Contracts;
using ReportingService.Repositories;

namespace ReportingService.Tests;

// The JobAssigned consume loop, run against a consumer the test controls rather
// than a broker. What is being pinned is the one decision the loop makes that a
// reader of the code cannot check by eye: which failures are committed past and
// which are read again.
//
// This is the second of the service's two loops. They run in two consumer groups
// and fail independently, which is what lets the jobs-by-status report stay
// current while the assignment projection is stuck, and the reverse.
public class JobAssignedConsumerTests
{
    private const string JobId = "9f1c7a24-8f4e-4c3a-9a52-2b6d0f5e1a77";
    private const string TechnicianId = "5c8a3f71-4b29-4e6d-8a03-9f1b7c2e4d58";

    // A well-formed JobAssigned, in the shape the Dispatch Service publishes -
    // six payload fields, camelCase, UTC timestamps.
    private static string ValidMessage(
        string jobId = JobId,
        string technicianId = TechnicianId,
        string technicianReference = "TECH-0001",
        string assignedAt = "2026-09-14T09:15:02.446Z") => $$"""
        {
          "eventId": "7b41e9c6-0d38-4a52-9f17-3c8b6e2d5a04",
          "eventType": "JobAssigned",
          "eventVersion": 1,
          "occurredAt": "{{assignedAt}}",
          "producer": "dispatch-service",
          "payload": {
            "assignmentId": "e2d7b415-9a63-4c08-b1f5-7d4e2a9c6301",
            "jobId": "{{jobId}}",
            "jobReference": "JOB-7K2M9X",
            "technicianId": "{{technicianId}}",
            "technicianReference": "{{technicianReference}}",
            "assignedAt": "{{assignedAt}}"
          }
        }
        """;

    // Runs the hosted service until the fake consumer runs out of messages, which
    // is what stands in for the host shutting down - the real loop only ends when
    // it is stopped.
    //
    // The wait is bounded so that a loop which stops making progress fails the
    // test instead of hanging the run.
    private static async Task RunAsync(
        CancellationTokenSource cts,
        FakeKafkaConsumer kafka,
        FakeJobProjectionRepository repository,
        Action<ConsumerConfig>? onConfig = null)
    {
        kafka.OnMessagesExhausted = cts.Cancel;

        // A real container rather than a fake scope factory: the loop opens one
        // scope per message, and that is the behaviour being relied on.
        await using var provider = new ServiceCollection()
            .AddScoped<IJobProjectionRepository>(_ => repository)
            .BuildServiceProvider();

        var consumer = new JobAssignedConsumer(
            "localhost:9092",
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<JobAssignedConsumer>.Instance,
            config =>
            {
                onConfig?.Invoke(config);

                return kafka;
            });

        await consumer.StartAsync(cts.Token);

        await consumer.ExecuteTask!.WaitAsync(TimeSpan.FromSeconds(30));
    }

    // ---- subscription ------------------------------------------------------

    [Fact]
    public async Task ExecuteAsync_SubscribesToJobAssignedWithItsOwnGroupAndManualCommits()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        var kafka = new FakeKafkaConsumer("job-assigned");
        var repository = new FakeJobProjectionRepository();
        ConsumerConfig? captured = null;

        // Act
        await RunAsync(cts, kafka, repository, config => captured = config);

        // Assert - its own group, distinct both from the Job Service's group on
        // this same topic and from this service's own job-created group. Sharing
        // either would make Kafka deliver each event to only one reader.
        Assert.Equal("assms-reporting-job-assigned", JobAssignedPayload.ConsumerGroup);
        Assert.Equal(JobAssignedPayload.ConsumerGroup, captured!.GroupId);
        Assert.NotEqual("assms-job-job-assigned", captured.GroupId);
        Assert.NotEqual(JobCreatedPayload.ConsumerGroup, captured.GroupId);

        Assert.Equal("job-assigned", Assert.Single(kafka.SubscribedTopics));

        Assert.False(captured.EnableAutoCommit);
        Assert.Equal(AutoOffsetReset.Earliest, captured.AutoOffsetReset);
    }

    [Fact]
    public async Task ExecuteAsync_OnShutdown_ClosesTheConsumer()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        var kafka = new FakeKafkaConsumer("job-assigned");
        var repository = new FakeJobProjectionRepository();

        // Act
        await RunAsync(cts, kafka, repository);

        // Assert
        Assert.Equal(1, kafka.CloseCallCount);
    }

    // ---- the happy path ----------------------------------------------------

    [Fact]
    public async Task ExecuteAsync_WithAValidMessage_ProjectsEveryColumnAndCommits()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        var kafka = new FakeKafkaConsumer("job-assigned");
        var repository = new FakeJobProjectionRepository();
        var message = kafka.Enqueue(ValidMessage());

        // Act
        await RunAsync(cts, kafka, repository);

        // Assert - the four columns job_assignment_projection stores, mapped out
        // of the six-field payload.
        var assignment = Assert.Single(repository.Assignments);

        Assert.Equal(JobId, assignment.JobId);
        Assert.Equal(TechnicianId, assignment.TechnicianId);
        Assert.Equal("TECH-0001", assignment.TechnicianReference);
        Assert.Equal(new DateTime(2026, 9, 14, 9, 15, 2, 446, DateTimeKind.Utc), assignment.AssignedAt);

        Assert.Equal(message.TopicPartitionOffset, Assert.Single(kafka.CommittedOffsets));
    }

    [Fact]
    public async Task ExecuteAsync_CommitsAfterTheWriteAndNotBefore()
    {
        // Arrange - the order matters: the write is allowed to go first precisely
        // because the upsert is idempotent, and a commit that ran first would
        // lose the assignment from the report entirely if the process then died.
        using var cts = new CancellationTokenSource();
        var kafka = new FakeKafkaConsumer("job-assigned");
        var repository = new FakeJobProjectionRepository();
        var committedWhenWritten = -1;

        repository.OnApplyAssignment = () => committedWhenWritten = kafka.CommittedOffsets.Count;

        kafka.Enqueue(ValidMessage());

        // Act
        await RunAsync(cts, kafka, repository);

        // Assert
        Assert.Equal(0, committedWhenWritten);
        Assert.Single(kafka.CommittedOffsets);
    }

    [Fact]
    public async Task ExecuteAsync_ReadsAssignedAtAsUtc()
    {
        // Arrange - the report filters on this column, so a timestamp read as the
        // server's local time would shift the report by the timezone of whatever
        // machine it happened to run on.
        using var cts = new CancellationTokenSource();
        var kafka = new FakeKafkaConsumer("job-assigned");
        var repository = new FakeJobProjectionRepository();

        kafka.Enqueue(ValidMessage());

        // Act
        await RunAsync(cts, kafka, repository);

        // Assert
        Assert.Equal(DateTimeKind.Utc, Assert.Single(repository.Assignments).AssignedAt.Kind);
    }

    // ---- duplicate safety --------------------------------------------------

    [Fact]
    public async Task ExecuteAsync_WithARedeliveredEvent_WritesItAgainRatherThanSkippingIt()
    {
        // Arrange - the same event twice, which is what at-least-once delivery
        // guarantees will happen. The consumer holds no in-memory record of what
        // it has seen; the upsert's primary key on job_id is the guard, and it
        // overwrites the row with identical values rather than double-counting.
        using var cts = new CancellationTokenSource();
        var kafka = new FakeKafkaConsumer("job-assigned");
        var repository = new FakeJobProjectionRepository();

        kafka.Enqueue(ValidMessage());
        kafka.Enqueue(ValidMessage());

        // Act
        await RunAsync(cts, kafka, repository);

        // Assert - both written, both committed, and both carrying the same job
        // id, which is what makes the second a no-op at the table.
        Assert.Equal(2, repository.ApplyAssignmentAsyncCallCount);
        Assert.Equal(2, kafka.CommittedOffsets.Count);
        Assert.All(repository.Assignments, assignment => Assert.Equal(JobId, assignment.JobId));
    }

    [Fact]
    public async Task ExecuteAsync_WithASecondAssignmentForTheSameJob_ProjectsTheLaterOne()
    {
        // Arrange - what US-15 reassignment will produce. Both rows key on the
        // same job id, so the projection holds one current assignment per job.
        using var cts = new CancellationTokenSource();
        var kafka = new FakeKafkaConsumer("job-assigned");
        var repository = new FakeJobProjectionRepository();

        kafka.Enqueue(ValidMessage(technicianReference: "TECH-0001", assignedAt: "2026-09-14T09:00:00.000Z"));
        kafka.Enqueue(ValidMessage(
            technicianId: "aaaa1111-bbbb-2222-cccc-333344445555",
            technicianReference: "TECH-0002",
            assignedAt: "2026-09-14T12:00:00.000Z"));

        // Act
        await RunAsync(cts, kafka, repository);

        // Assert - both reached the projection in arrival order, carrying their
        // own technician and timestamp. Which one survives is the table's
        // decision, not the loop's.
        Assert.Equal(2, repository.Assignments.Count);
        Assert.Equal("TECH-0001", repository.Assignments[0].TechnicianReference);
        Assert.Equal("TECH-0002", repository.Assignments[1].TechnicianReference);
        Assert.Equal(JobId, repository.Assignments[1].JobId);
    }

    // ---- malformed events: commit past -------------------------------------

    [Fact]
    public async Task ExecuteAsync_WithAMalformedMessage_CommitsPastItWithoutWriting()
    {
        // Arrange - JSON that will never parse. Not committing it would stop the
        // projection dead on that one message and every event behind it.
        using var cts = new CancellationTokenSource();
        var kafka = new FakeKafkaConsumer("job-assigned");
        var repository = new FakeJobProjectionRepository();
        var message = kafka.Enqueue("{ not json");

        // Act
        await RunAsync(cts, kafka, repository);

        // Assert
        Assert.Empty(repository.Assignments);
        Assert.Equal(message.TopicPartitionOffset, Assert.Single(kafka.CommittedOffsets));
        Assert.Empty(kafka.SeekedOffsets);
    }

    [Fact]
    public async Task ExecuteAsync_WithAMalformedMessage_KeepsProcessingTheOnesBehindIt()
    {
        // Arrange - the reason committing past a bad message matters at all.
        using var cts = new CancellationTokenSource();
        var kafka = new FakeKafkaConsumer("job-assigned");
        var repository = new FakeJobProjectionRepository();

        kafka.Enqueue("{ not json");
        kafka.Enqueue(ValidMessage());

        // Act
        await RunAsync(cts, kafka, repository);

        // Assert
        Assert.Single(repository.Assignments);
        Assert.Equal(2, kafka.CommittedOffsets.Count);
    }

    [Theory]
    // A bare null: valid JSON, not an event.
    [InlineData("null")]
    // An envelope with no payload at all.
    [InlineData("""{"eventId":"a","eventType":"JobAssigned","eventVersion":1,"producer":"dispatch-service"}""")]
    // The three fields the projection's NOT NULL columns cannot be written
    // without, blanked in turn.
    [InlineData("""{"payload":{"jobId":"","technicianId":"t","technicianReference":"r","assignedAt":"2026-09-14T09:15:02.446Z"}}""")]
    [InlineData("""{"payload":{"jobId":"j","technicianId":"","technicianReference":"r","assignedAt":"2026-09-14T09:15:02.446Z"}}""")]
    [InlineData("""{"payload":{"jobId":"j","technicianId":"t","technicianReference":"","assignedAt":"2026-09-14T09:15:02.446Z"}}""")]
    public async Task ExecuteAsync_WithAnInvalidEvent_CommitsPastItWithoutWriting(string body)
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        var kafka = new FakeKafkaConsumer("job-assigned");
        var repository = new FakeJobProjectionRepository();
        var message = kafka.Enqueue(body);

        // Act
        await RunAsync(cts, kafka, repository);

        // Assert - treated exactly as malformed: no retry will make a missing
        // field appear.
        Assert.Empty(repository.Assignments);
        Assert.Equal(message.TopicPartitionOffset, Assert.Single(kafka.CommittedOffsets));
        Assert.Empty(kafka.SeekedOffsets);
    }

    // ---- transient database failure: seek and retry ------------------------

    [Fact]
    public async Task ExecuteAsync_WhenTheWriteFails_SeeksBackToTheMessageAndDoesNotCommit()
    {
        // Arrange - a write that failed for a reason outside the message. The same
        // message will succeed once the database is back, so its offset must not
        // move.
        using var cts = new CancellationTokenSource();
        var kafka = new FakeKafkaConsumer("job-assigned");
        var repository = new FakeJobProjectionRepository
        {
            ApplyAssignmentExceptionToThrow = new InvalidOperationException("The database was unreachable.")
        };

        // Cancelling at the moment of failure unwinds the loop instead of sleeping
        // out its five-second retry delay.
        repository.OnApplyAssignment = cts.Cancel;

        var message = kafka.Enqueue(ValidMessage());

        // Act
        await RunAsync(cts, kafka, repository);

        // Assert - seeked, not committed.
        Assert.Equal(message.TopicPartitionOffset, Assert.Single(kafka.SeekedOffsets));
        Assert.Empty(kafka.CommittedOffsets);
    }

    [Fact]
    public async Task ExecuteAsync_WhenTheWriteFailsAndThenSucceeds_ProjectsTheMessageAndCommitsIt()
    {
        // Arrange - a database that was down and came back, which is the case the
        // retry exists for. The fake's Seek re-queues the message, as the real
        // consumer would redeliver it.
        using var cts = new CancellationTokenSource();
        var kafka = new FakeKafkaConsumer("job-assigned");
        var repository = new FakeJobProjectionRepository { ApplyAssignmentFailuresBeforeSuccess = 1 };
        var message = kafka.Enqueue(ValidMessage());

        // Act
        await RunAsync(cts, kafka, repository);

        // Assert
        Assert.Equal(2, repository.ApplyAssignmentAsyncCallCount);
        Assert.Equal(message.TopicPartitionOffset, Assert.Single(kafka.SeekedOffsets));
        Assert.Equal(message.TopicPartitionOffset, Assert.Single(kafka.CommittedOffsets));
        Assert.Equal(JobId, repository.Assignments[^1].JobId);
    }

    [Fact]
    public async Task ExecuteAsync_WhenConsumingFails_KeepsReading()
    {
        // Arrange - a broker or transport failure, which is neither a bad message
        // nor a failed write. There is nothing to commit and nothing to skip.
        using var cts = new CancellationTokenSource();
        var kafka = new FakeKafkaConsumer("job-assigned")
        {
            ConsumeExceptionToThrow = new ConsumeException(
                new ConsumeResult<byte[], byte[]>(),
                new Error(ErrorCode.Local_Transport, "The broker was unreachable."))
        };
        var repository = new FakeJobProjectionRepository();

        kafka.Enqueue(ValidMessage());

        // Act
        await RunAsync(cts, kafka, repository);

        // Assert - the message behind the failure was still projected.
        Assert.Single(repository.Assignments);
        Assert.Single(kafka.CommittedOffsets);
    }

    // ---- the contract ------------------------------------------------------

    [Fact]
    public void PayloadType_DeclaresExactlyTheSixFieldsDispatchPublishes()
    {
        // Each service redefines the payload independently, so nothing at compile
        // time catches a field that has drifted - a renamed field simply
        // deserializes as null forever. This guards the count and the names
        // directly, against the six Dispatch actually serializes.
        var declared = typeof(JobAssignedPayload)
            .GetProperties()
            .Select(property => property.Name)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(
            new[]
            {
                "AssignedAt",
                "AssignmentId",
                "JobId",
                "JobReference",
                "TechnicianId",
                "TechnicianReference"
            },
            declared);
    }

    [Fact]
    public void Deserialize_ReadsEveryFieldDispatchPublishes()
    {
        var envelope = JobAssignedConsumer.Deserialize(ValidMessage());

        Assert.Equal("e2d7b415-9a63-4c08-b1f5-7d4e2a9c6301", envelope!.Payload.AssignmentId);
        Assert.Equal(JobId, envelope.Payload.JobId);
        Assert.Equal("JOB-7K2M9X", envelope.Payload.JobReference);
        Assert.Equal(TechnicianId, envelope.Payload.TechnicianId);
        Assert.Equal("TECH-0001", envelope.Payload.TechnicianReference);
        Assert.Equal(new DateTime(2026, 9, 14, 9, 15, 2, 446, DateTimeKind.Utc), envelope.Payload.AssignedAt);

        // The envelope's five non-payload fields are part of the contract too.
        Assert.Equal("JobAssigned", envelope.EventType);
        Assert.Equal(1, envelope.EventVersion);
        Assert.Equal("dispatch-service", envelope.Producer);
    }

    [Fact]
    public void Deserialize_ToleratesAFieldAddedAfterThisServiceWasWritten()
    {
        // The contract's versioning rule: adding an optional field does not change
        // eventVersion, so consumers must ignore fields they do not recognise
        // rather than failing on them.
        const string withExtraField = """
            {
              "eventType": "JobAssigned",
              "eventVersion": 1,
              "producer": "dispatch-service",
              "payload": {
                "assignmentId": "e2d7b415-9a63-4c08-b1f5-7d4e2a9c6301",
                "jobId": "9f1c7a24-8f4e-4c3a-9a52-2b6d0f5e1a77",
                "jobReference": "JOB-7K2M9X",
                "technicianId": "5c8a3f71-4b29-4e6d-8a03-9f1b7c2e4d58",
                "technicianReference": "TECH-0001",
                "assignedAt": "2026-09-14T09:15:02.446Z",
                "dispatcherNote": "added by a later story"
              }
            }
            """;

        Assert.Equal(JobId, JobAssignedConsumer.Deserialize(withExtraField)!.Payload.JobId);
    }

    [Fact]
    public void Deserialize_OfMalformedJson_Throws()
    {
        // The loop distinguishes a throw here from a failed write: this one is
        // committed past, that one is read again. The distinction only holds if
        // malformed input actually throws.
        Assert.Throws<System.Text.Json.JsonException>(() => JobAssignedConsumer.Deserialize("{ not json"));
    }
}
