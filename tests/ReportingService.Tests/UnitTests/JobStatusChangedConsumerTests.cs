using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using ReportingService.Messaging.Consumers;
using ReportingService.Messaging.Contracts;
using ReportingService.Repositories;

namespace ReportingService.Tests;

public class JobStatusChangedConsumerTests
{
    private const string JobId = "9f1c7a24-8f4e-4c3a-9a52-2b6d0f5e1a77";
    private const string TechnicianId = "5c8a3f71-4b29-4e6d-8a03-9f1b7c2e4d58";

    private static string ValidMessage(
        string jobId = JobId,
        string newStatus = "COMPLETED",
        string oldStatus = "IN_PROGRESS",
        string technicianId = TechnicianId,
        string technicianReference = "TECH-0001",
        string occurredAt = "2026-09-22T08:00:00.000Z") => $$"""
        {
          "eventId": "7b41e9c6-0d38-4a52-9f17-3c8b6e2d5a04",
          "eventType": "JobStatusChanged",
          "eventVersion": 1,
          "occurredAt": "{{occurredAt}}",
          "producer": "job-service",
          "payload": {
            "jobId": "{{jobId}}",
            "jobReference": "JOB-7K2M9X",
            "assignmentId": "e2d7b415-9a63-4c08-b1f5-7d4e2a9c6301",
            "technicianId": "{{technicianId}}",
            "technicianReference": "{{technicianReference}}",
            "oldStatus": "{{oldStatus}}",
            "newStatus": "{{newStatus}}",
            "occurredAt": "{{occurredAt}}"
          }
        }
        """;

    private static async Task RunAsync(
        CancellationTokenSource cts,
        FakeKafkaConsumer kafka,
        FakeJobProjectionRepository repository,
        Action<ConsumerConfig>? onConfig = null)
    {
        kafka.OnMessagesExhausted = cts.Cancel;

        await using var provider = new ServiceCollection()
            .AddScoped<IJobProjectionRepository>(_ => repository)
            .BuildServiceProvider();

        var consumer = new JobStatusChangedConsumer(
            "localhost:9092",
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<JobStatusChangedConsumer>.Instance,
            config =>
            {
                onConfig?.Invoke(config);
                return kafka;
            });

        await consumer.StartAsync(cts.Token);
        await consumer.ExecuteTask!.WaitAsync(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public async Task SubscribesToJobStatusChangedTopicWithDedicatedConsumerGroup()
    {
        using var cts = new CancellationTokenSource();
        var kafka = new FakeKafkaConsumer(JobStatusChangedPayload.Topic);
        var repository = new FakeJobProjectionRepository();
        ConsumerConfig? capturedConfig = null;

        await RunAsync(cts, kafka, repository, config => capturedConfig = config);

        Assert.Equal(JobStatusChangedPayload.Topic, Assert.Single(kafka.SubscribedTopics));
        Assert.NotNull(capturedConfig);
        Assert.Equal(JobStatusChangedPayload.ConsumerGroup, capturedConfig.GroupId);
        Assert.False(capturedConfig.EnableAutoCommit);
    }

    [Fact]
    public async Task ProjectsValidJobStatusChangedEventAndCommitsOffset()
    {
        using var cts = new CancellationTokenSource();
        var kafka = new FakeKafkaConsumer(JobStatusChangedPayload.Topic);
        var message = kafka.Enqueue(ValidMessage());

        var repository = new FakeJobProjectionRepository();

        await RunAsync(cts, kafka, repository);

        Assert.Equal(1, repository.ApplyJobStatusChangedAsyncCallCount);
        var change = Assert.Single(repository.StatusChanges);
        Assert.Equal(JobId, change.JobId);
        Assert.Equal("JOB-7K2M9X", change.JobReference);
        Assert.Equal("COMPLETED", change.NewStatus);
        Assert.Equal("IN_PROGRESS", change.OldStatus);
        Assert.Equal(TechnicianId, change.TechnicianId);
        Assert.Equal("TECH-0001", change.TechnicianReference);

        Assert.Equal(message.TopicPartitionOffset, Assert.Single(kafka.CommittedOffsets));
    }

    [Fact]
    public async Task DiscardsMalformedJsonAndCommitsPastIt()
    {
        using var cts = new CancellationTokenSource();
        var kafka = new FakeKafkaConsumer(JobStatusChangedPayload.Topic);
        var message = kafka.Enqueue("{ not json");

        var repository = new FakeJobProjectionRepository();

        await RunAsync(cts, kafka, repository);

        Assert.Equal(0, repository.ApplyJobStatusChangedAsyncCallCount);
        Assert.Equal(message.TopicPartitionOffset, Assert.Single(kafka.CommittedOffsets));
    }

    [Fact]
    public async Task DiscardsInvalidEventAndCommitsPastIt()
    {
        using var cts = new CancellationTokenSource();
        var kafka = new FakeKafkaConsumer(JobStatusChangedPayload.Topic);
        // Missing jobId
        var message = kafka.Enqueue(ValidMessage(jobId: "not-a-guid"), key: "wrong-key");

        var repository = new FakeJobProjectionRepository();

        await RunAsync(cts, kafka, repository);

        Assert.Equal(0, repository.ApplyJobStatusChangedAsyncCallCount);
        Assert.Equal(message.TopicPartitionOffset, Assert.Single(kafka.CommittedOffsets));
    }

    [Fact]
    public async Task SeeksBackOnTransientRepositoryFailure()
    {
        using var cts = new CancellationTokenSource();
        var kafka = new FakeKafkaConsumer(JobStatusChangedPayload.Topic);
        var message = kafka.Enqueue(ValidMessage());

        var repository = new FakeJobProjectionRepository
        {
            ApplyJobStatusChangedFailuresBeforeSuccess = 1,
            OnApplyJobStatusChanged = () => cts.Cancel()
        };

        await RunAsync(cts, kafka, repository);

        Assert.Equal(1, repository.ApplyJobStatusChangedAsyncCallCount);
        Assert.Empty(kafka.CommittedOffsets);
        Assert.Equal(message.TopicPartitionOffset, Assert.Single(kafka.SeekedOffsets));
    }
}
