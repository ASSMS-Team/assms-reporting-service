using Confluent.Kafka;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

using ReportingService.Messaging.Consumers;
using ReportingService.Messaging.Contracts;
using ReportingService.Repositories;

namespace ReportingService.Tests;

// The consume loop, run against a consumer the test controls rather than a
// broker. What is being pinned is the one decision the loop makes that a reader
// of the code cannot check by eye: which failures are committed past and which
// are read again.
public class JobCreatedConsumerTests
{
    private const string JobId = "9f1c7a24-8f4e-4c3a-9a52-2b6d0f5e1a77";

    // A well-formed JobCreated, in the shape the Job Service publishes.
    private static string ValidMessage(
        string jobId = JobId,
        string status = "CREATED",
        string createdAt = "2026-08-29T09:14:32.118Z") => $$"""
        {
          "eventId": "3d2a1b90-6c77-4f18-b0a1-5c9e7d4a2f31",
          "eventType": "JobCreated",
          "eventVersion": 1,
          "occurredAt": "{{createdAt}}",
          "producer": "job-service",
          "payload": {
            "jobId": "{{jobId}}",
            "jobReference": "JOB-7K2M9X",
            "customerId": "c41b9e2d-77a3-4d5f-8e10-6b2c9a0f4d13",
            "assetId": "a70e5c18-2d94-4b6a-9f37-1e8d5c3b7a62",
            "serviceCategory": "REPAIR",
            "problemDescription": "Not cooling and trips the breaker after ten minutes.",
            "priority": "HIGH",
            "region": "WESTERN",
            "status": "{{status}}",
            "createdAt": "{{createdAt}}"
          }
        }
        """;

    // Runs the hosted service until the fake consumer runs out of messages,
    // which is what stands in for the host shutting down - the real loop only
    // ends when it is stopped.
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
            .AddSingleton<IJobProjectionRepository>(repository)
            .BuildServiceProvider();

        var consumer = new JobCreatedConsumer(
            "localhost:9092",
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<JobCreatedConsumer>.Instance,
            config =>
            {
                onConfig?.Invoke(config);

                return kafka;
            });

        await consumer.StartAsync(cts.Token);

        await consumer.ExecuteTask!.WaitAsync(TimeSpan.FromSeconds(30));
    }

    // ---- wiring ------------------------------------------------------------

    [Fact]
    public async Task ExecuteAsync_SubscribesToJobCreatedWithTheContractsGroupAndManualCommits()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        var kafka = new FakeKafkaConsumer();
        var repository = new FakeJobProjectionRepository();

        ConsumerConfig? config = null;

        // Act
        await RunAsync(cts, kafka, repository, captured => config = captured);

        // Assert - none of these is a build failure or a runtime error when it
        // is wrong. Auto-commit left on would move the offset on a timer with no
        // idea whether the write succeeded, and Latest would silently skip every
        // job raised before this service first ran.
        Assert.Equal(JobCreatedPayload.Topic, Assert.Single(kafka.SubscribedTopics));

        Assert.NotNull(config);
        Assert.Equal("assms-reporting-job-created", config!.GroupId);
        Assert.Equal(JobCreatedPayload.ConsumerGroup, config.GroupId);
        Assert.False(config.EnableAutoCommit);
        Assert.Equal(AutoOffsetReset.Earliest, config.AutoOffsetReset);
        Assert.Equal("localhost:9092", config.BootstrapServers);
    }

    [Fact]
    public async Task ExecuteAsync_OnShutdown_ClosesTheConsumer()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        var kafka = new FakeKafkaConsumer();
        var repository = new FakeJobProjectionRepository();

        // Act
        await RunAsync(cts, kafka, repository);

        // Assert - leaving the group deliberately is what gets the partitions
        // reassigned straight away instead of after the session timeout.
        Assert.Equal(1, kafka.CloseCallCount);
    }

    // ---- the happy path ----------------------------------------------------

    [Fact]
    public async Task ExecuteAsync_WithAValidMessage_ProjectsEveryColumnAndCommits()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        var kafka = new FakeKafkaConsumer();
        var repository = new FakeJobProjectionRepository();

        var message = kafka.Enqueue(ValidMessage());

        // Act
        await RunAsync(cts, kafka, repository);

        // Assert
        var written = Assert.Single(repository.Upserted);

        Assert.Equal(JobId, written.JobId);
        Assert.Equal("JOB-7K2M9X", written.JobReference);
        Assert.Equal("CREATED", written.Status);
        Assert.Equal("WESTERN", written.Region);
        Assert.Equal("HIGH", written.Priority);
        Assert.Equal("REPAIR", written.ServiceCategory);

        // The event's createdAt, in UTC - the job's creation time upstream, not
        // the time this row was written. projected_at is the database's to fill
        // in, which is why nothing here sets it.
        Assert.Equal(new DateTime(2026, 8, 29, 9, 14, 32, 118), written.JobCreatedAt);
        Assert.Equal(DateTimeKind.Utc, written.JobCreatedAt.Kind);

        Assert.Equal(message.TopicPartitionOffset, Assert.Single(kafka.CommittedOffsets));
    }

    [Fact]
    public async Task ExecuteAsync_CommitsAfterTheWriteAndNotBefore()
    {
        // Arrange - the ordering is the point. Committing first loses the job
        // whenever the process dies in between; committing last risks writing
        // the row twice, which the idempotent upsert makes harmless.
        using var cts = new CancellationTokenSource();
        var kafka = new FakeKafkaConsumer();
        var repository = new FakeJobProjectionRepository();

        var committedWhenTheWriteRan = -1;
        repository.OnUpsert = () => committedWhenTheWriteRan = kafka.CommittedOffsets.Count;

        kafka.Enqueue(ValidMessage());

        // Act
        await RunAsync(cts, kafka, repository);

        // Assert - nothing was committed at the moment the write ran, and
        // something was committed by the end.
        Assert.Equal(0, committedWhenTheWriteRan);
        Assert.Single(kafka.CommittedOffsets);
    }

    [Fact]
    public async Task ExecuteAsync_WithARedeliveredEvent_WritesItAgainRatherThanSkippingIt()
    {
        // Arrange - Kafka delivers at least once, so the same event arriving
        // twice is normal rather than exceptional. The consumer does not
        // de-duplicate; it writes both times and lets the primary key collapse
        // them, which is what keeps the report's counts right.
        using var cts = new CancellationTokenSource();
        var kafka = new FakeKafkaConsumer();
        var repository = new FakeJobProjectionRepository();

        kafka.Enqueue(ValidMessage());
        kafka.Enqueue(ValidMessage());

        // Act
        await RunAsync(cts, kafka, repository);

        // Assert - two writes of the same job id, and both offsets committed.
        Assert.Equal(2, repository.UpsertAsyncCallCount);
        Assert.All(repository.Upserted, projection => Assert.Equal(JobId, projection.JobId));
        Assert.Equal(2, kafka.CommittedOffsets.Count);
    }

    [Fact]
    public async Task ExecuteAsync_WithAStatusTheServiceDoesNotKnow_ProjectsItUnchanged()
    {
        // Arrange - a value added to the job lifecycle upstream. Nothing in this
        // service, and nothing in the table, is allowed to reject it: see the
        // no-CHECK-constraint reasoning in the migration.
        using var cts = new CancellationTokenSource();
        var kafka = new FakeKafkaConsumer();
        var repository = new FakeJobProjectionRepository();

        kafka.Enqueue(ValidMessage(status: "AWAITING_PARTS"));

        // Act
        await RunAsync(cts, kafka, repository);

        // Assert
        Assert.Equal("AWAITING_PARTS", Assert.Single(repository.Upserted).Status);
        Assert.Single(kafka.CommittedOffsets);
    }

    // ---- malformed messages: commit past them ------------------------------

    [Theory]
    [InlineData("this is not json")]
    [InlineData("{ \"eventType\": \"JobCreated\"")]
    [InlineData("null")]
    [InlineData("{\"eventType\":\"JobCreated\",\"eventVersion\":1}")]
    public async Task ExecuteAsync_WithAMalformedMessage_CommitsPastItWithoutWriting(string value)
    {
        // Arrange - unparseable JSON, and JSON that parses but carries no
        // payload. Neither will ever succeed, so neither may be retried.
        using var cts = new CancellationTokenSource();
        var kafka = new FakeKafkaConsumer();
        var repository = new FakeJobProjectionRepository();

        var message = kafka.Enqueue(value);

        // Act
        await RunAsync(cts, kafka, repository);

        // Assert - committed, so the loop moves past it; not written, because
        // there was nothing to write; not sought, because re-reading it would
        // only produce the same failure.
        Assert.Equal(message.TopicPartitionOffset, Assert.Single(kafka.CommittedOffsets));
        Assert.Equal(0, repository.UpsertAsyncCallCount);
        Assert.Empty(kafka.SeekedOffsets);
    }

    [Fact]
    public async Task ExecuteAsync_WithAMalformedMessage_KeepsProcessingTheOnesBehindIt()
    {
        // Arrange - this is what committing past a poison message buys. Retrying
        // it forever would block not just this event but every later one, and a
        // hand-typed smoke test on the topic would stop the report permanently.
        using var cts = new CancellationTokenSource();
        var kafka = new FakeKafkaConsumer();
        var repository = new FakeJobProjectionRepository();

        kafka.Enqueue("this is not json");
        kafka.Enqueue(ValidMessage());

        // Act
        await RunAsync(cts, kafka, repository);

        // Assert - the good message behind it was projected, and both offsets
        // moved on.
        Assert.Equal(JobId, Assert.Single(repository.Upserted).JobId);
        Assert.Equal(2, kafka.CommittedOffsets.Count);
    }

    // ---- failed writes: read them again ------------------------------------

    [Fact]
    public async Task ExecuteAsync_WhenTheWriteFails_SeeksBackToTheMessageAndDoesNotCommit()
    {
        // Arrange - the database is unreachable. Cancelling at the moment of
        // failure stands in for the host stopping, so the loop unwinds instead
        // of sitting out its retry delay.
        using var cts = new CancellationTokenSource();
        var kafka = new FakeKafkaConsumer();
        var repository = new FakeJobProjectionRepository
        {
            UpsertExceptionToThrow = new InvalidOperationException("The database was unreachable.")
        };
        repository.OnUpsert = cts.Cancel;

        var message = kafka.Enqueue(ValidMessage());

        // Act
        await RunAsync(cts, kafka, repository);

        // Assert - the offset stays where it is, so the event is not lost.
        Assert.Empty(kafka.CommittedOffsets);

        // And the seek is what actually causes the retry. Without it the
        // consumer's position has already moved past this message, so not
        // committing on its own would leave the event unread until a rebalance.
        Assert.Equal(message.TopicPartitionOffset, Assert.Single(kafka.SeekedOffsets));
    }

    [Fact]
    public async Task ExecuteAsync_WhenTheWriteFailsAndThenSucceeds_ProjectsTheMessageAndCommitsIt()
    {
        // Arrange - the database was down for one attempt and came back.
        //
        // This test really does wait out the consumer's retry delay, so it takes
        // a few seconds. That delay is deliberate: without it a database that is
        // down turns the retry into a hot loop.
        using var cts = new CancellationTokenSource();
        var kafka = new FakeKafkaConsumer();
        var repository = new FakeJobProjectionRepository { UpsertFailuresBeforeSuccess = 1 };

        var message = kafka.Enqueue(ValidMessage());

        // Act
        await RunAsync(cts, kafka, repository);

        // Assert - written twice because it was read twice, and committed only
        // once the write finally went through.
        Assert.Equal(2, repository.UpsertAsyncCallCount);
        Assert.Equal(JobId, repository.Upserted[1].JobId);

        Assert.Equal(message.TopicPartitionOffset, Assert.Single(kafka.SeekedOffsets));
        Assert.Equal(message.TopicPartitionOffset, Assert.Single(kafka.CommittedOffsets));
    }

    // ---- broker failures ---------------------------------------------------

    [Fact]
    public async Task ExecuteAsync_WhenConsumingFails_KeepsReading()
    {
        // Arrange - a transport failure is neither a bad message nor a failed
        // write: there is nothing to commit and nothing to skip, and the client
        // reconnects on its own. The loop must not treat it as either of the
        // other two.
        using var cts = new CancellationTokenSource();
        var kafka = new FakeKafkaConsumer
        {
            ConsumeExceptionToThrow = new ConsumeException(
                new ConsumeResult<byte[], byte[]>(),
                new Error(ErrorCode.Local_Transport, "Broker transport failure"))
        };
        var repository = new FakeJobProjectionRepository();

        kafka.Enqueue(ValidMessage());

        // Act
        await RunAsync(cts, kafka, repository);

        // Assert - the message queued behind the failure was still projected,
        // and nothing was committed or skipped on account of the failure itself.
        Assert.Equal(JobId, Assert.Single(repository.Upserted).JobId);
        Assert.Single(kafka.CommittedOffsets);
        Assert.Empty(kafka.SeekedOffsets);
    }
}
