using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using ReportingService.Messaging.Consumers;
using ReportingService.Messaging.Contracts;
using ReportingService.Models;
using ReportingService.Repositories;
using Xunit;

namespace ReportingService.Tests;

public class JobProjectionConsumersMockTests
{
    private const string SampleJobId = "9f1c7a24-8f4e-4c3a-9a52-2b6d0f5e1a77";
    private const string SampleTechnicianId = "8e2b6c13-7d3f-4b2a-8941-1a5c0e4d0f66";
    private const string SampleAssignmentId = "7d1a5b02-6c2e-3a1f-7830-0f4b9d3c9e55";

    private readonly Mock<IServiceScopeFactory> _scopeFactoryMock;
    private readonly Mock<IServiceScope> _scopeMock;
    private readonly Mock<IServiceProvider> _serviceProviderMock;
    private readonly Mock<IJobProjectionRepository> _repositoryMock;

    public JobProjectionConsumersMockTests()
    {
        _scopeFactoryMock = new Mock<IServiceScopeFactory>(MockBehavior.Strict);
        _scopeMock = new Mock<IServiceScope>(MockBehavior.Strict);
        _serviceProviderMock = new Mock<IServiceProvider>(MockBehavior.Strict);
        _repositoryMock = new Mock<IJobProjectionRepository>(MockBehavior.Strict);

        _scopeFactoryMock.Setup(f => f.CreateScope()).Returns(_scopeMock.Object);
        _scopeMock.Setup(s => s.ServiceProvider).Returns(_serviceProviderMock.Object);
        _scopeMock.Setup(s => s.Dispose());
        _serviceProviderMock
            .Setup(p => p.GetService(typeof(IJobProjectionRepository)))
            .Returns(_repositoryMock.Object);
    }

    private static string BuildJobCreatedJson(string jobId = SampleJobId, string jobRef = "JOB-000001") =>
        $$"""
        {
          "eventId": "11111111-1111-1111-1111-111111111111",
          "eventType": "JobCreated",
          "eventVersion": 1,
          "occurredAt": "2026-08-01T09:00:00Z",
          "producer": "job-service",
          "payload": {
            "jobId": "{{jobId}}",
            "jobReference": "{{jobRef}}",
            "customerId": "22222222-2222-2222-2222-222222222222",
            "assetId": "33333333-3333-3333-3333-333333333333",
            "serviceCategory": "REPAIR",
            "problemDescription": "AC broken",
            "priority": "HIGH",
            "region": "WESTERN",
            "status": "CREATED",
            "createdAt": "2026-08-01T09:00:00Z"
          }
        }
        """;

    private static string BuildJobAssignedJson(string jobId = SampleJobId, string techId = SampleTechnicianId) =>
        $$"""
        {
          "eventId": "44444444-4444-4444-4444-444444444444",
          "eventType": "JobAssigned",
          "eventVersion": 1,
          "occurredAt": "2026-08-01T10:00:00Z",
          "producer": "dispatch-service",
          "payload": {
            "jobId": "{{jobId}}",
            "jobReference": "JOB-000001",
            "assignmentId": "{{SampleAssignmentId}}",
            "technicianId": "{{techId}}",
            "technicianReference": "TEC-001",
            "assignedAt": "2026-08-01T10:00:00Z"
          }
        }
        """;

    private static string BuildJobStatusChangedJson(string jobId = SampleJobId, string status = "COMPLETED") =>
        $$"""
        {
          "eventId": "55555555-5555-5555-5555-555555555555",
          "eventType": "JobStatusChanged",
          "eventVersion": 1,
          "occurredAt": "2026-08-01T12:00:00Z",
          "producer": "job-service",
          "payload": {
            "jobId": "{{jobId}}",
            "jobReference": "JOB-000001",
            "assignmentId": "{{SampleAssignmentId}}",
            "technicianId": "{{SampleTechnicianId}}",
            "technicianReference": "TEC-001",
            "oldStatus": "IN_PROGRESS",
            "newStatus": "{{status}}",
            "occurredAt": "2026-08-01T12:00:00Z"
          }
        }
        """;

    #region JobCreatedConsumer Tests

    [Theory]
    [InlineData("JOB-A1B2C3", "WESTERN", "REPAIR")]
    [InlineData("JOB-D4E5F6", "CENTRAL", "INSTALLATION")]
    public void JobCreatedConsumer_Deserialize_ValidJson_ReturnsParsedPayload(string jobRef, string region, string category)
    {
        // Arrange
        var json = $$"""
        {
          "eventId": "11111111-1111-1111-1111-111111111111",
          "eventType": "JobCreated",
          "eventVersion": 1,
          "occurredAt": "2026-08-01T09:00:00Z",
          "producer": "job-service",
          "payload": {
            "jobId": "{{SampleJobId}}",
            "jobReference": "{{jobRef}}",
            "customerId": "22222222-2222-2222-2222-222222222222",
            "assetId": "33333333-3333-3333-3333-333333333333",
            "serviceCategory": "{{category}}",
            "problemDescription": "Test issue",
            "priority": "HIGH",
            "region": "{{region}}",
            "status": "CREATED",
            "createdAt": "2026-08-01T09:00:00Z"
          }
        }
        """;

        // Act
        var payload = JobCreatedConsumer.Deserialize(json);

        // Assert
        Assert.NotNull(payload);
        Assert.Equal(SampleJobId, payload.JobId);
        Assert.Equal(jobRef, payload.JobReference);
        Assert.Equal(region, payload.Region);
        Assert.Equal(category, payload.ServiceCategory);
    }

    [Theory]
    [InlineData("not valid json")]
    [InlineData("{broken}")]
    [InlineData("{\"eventId\": ")]
    public void JobCreatedConsumer_Deserialize_MalformedJson_ThrowsJsonException(string malformedJson)
    {
        Assert.Throws<JsonException>(() => JobCreatedConsumer.Deserialize(malformedJson));
    }

    [Fact]
    public async Task JobCreatedConsumer_ValidMessage_CallsRepositoryUpsertAsyncAndCommits()
    {
        // Arrange
        var fakeConsumer = new FakeKafkaConsumer();
        var cts = new CancellationTokenSource();
        fakeConsumer.OnMessagesExhausted = () => cts.Cancel();
        fakeConsumer.Enqueue(BuildJobCreatedJson(), key: SampleJobId);

        _repositoryMock
            .Setup(r => r.UpsertAsync(It.Is<JobProjection>(p =>
                p.JobId == SampleJobId &&
                p.JobReference == "JOB-000001" &&
                p.Region == "WESTERN")))
            .Returns(Task.CompletedTask);

        var consumer = new JobCreatedConsumer(
            "localhost:9092",
            _scopeFactoryMock.Object,
            NullLogger<JobCreatedConsumer>.Instance,
            _ => fakeConsumer);

        // Act
        await consumer.StartAsync(CancellationToken.None);
        await Task.Delay(200);
        await consumer.StopAsync(CancellationToken.None);

        // Assert
        _repositoryMock.Verify(r => r.UpsertAsync(It.IsAny<JobProjection>()), Times.Once);
        Assert.Single(fakeConsumer.CommittedOffsets);
    }

    #endregion

    #region JobAssignedConsumer Tests

    [Fact]
    public void JobAssignedConsumer_Deserialize_ValidJson_ReturnsEnvelope()
    {
        // Act
        var envelope = JobAssignedConsumer.Deserialize(BuildJobAssignedJson());

        // Assert
        Assert.NotNull(envelope);
        Assert.NotNull(envelope.Payload);
        Assert.Equal(SampleJobId, envelope.Payload.JobId);
        Assert.Equal(SampleTechnicianId, envelope.Payload.TechnicianId);
    }

    [Theory]
    [InlineData(SampleJobId, true)]
    [InlineData("mismatched-key", false)]
    [InlineData(null, false)]
    public void JobAssignedConsumer_IsUsable_ValidatesMessageKeyAgainstPayloadJobId(string? key, bool expectedUsable)
    {
        // Arrange
        var envelope = JobAssignedConsumer.Deserialize(BuildJobAssignedJson());

        // Act
        var usable = JobAssignedConsumer.IsUsable(envelope, key);

        // Assert
        Assert.Equal(expectedUsable, usable);
    }

    [Fact]
    public async Task JobAssignedConsumer_UsableMessage_CallsApplyAssignmentAndCommits()
    {
        // Arrange
        var fakeConsumer = new FakeKafkaConsumer("job-assigned");
        var cts = new CancellationTokenSource();
        fakeConsumer.OnMessagesExhausted = () => cts.Cancel();
        fakeConsumer.Enqueue(BuildJobAssignedJson(), key: SampleJobId);

        _repositoryMock
            .Setup(r => r.ApplyAssignmentAsync(It.Is<JobAssignmentProjection>(a =>
                a.JobId == SampleJobId &&
                a.TechnicianId == SampleTechnicianId &&
                a.TechnicianReference == "TEC-001")))
            .Returns(Task.CompletedTask);

        var consumer = new JobAssignedConsumer(
            "localhost:9092",
            _scopeFactoryMock.Object,
            NullLogger<JobAssignedConsumer>.Instance,
            _ => fakeConsumer);

        // Act
        await consumer.StartAsync(CancellationToken.None);
        await Task.Delay(200);
        await consumer.StopAsync(CancellationToken.None);

        // Assert
        _repositoryMock.Verify(r => r.ApplyAssignmentAsync(It.IsAny<JobAssignmentProjection>()), Times.Once);
        Assert.Single(fakeConsumer.CommittedOffsets);
    }

    [Fact]
    public async Task JobAssignedConsumer_UnusableMessage_DiscardsAndCommitsWithoutCallingRepository()
    {
        // Arrange (Key does not match payload jobId)
        var fakeConsumer = new FakeKafkaConsumer("job-assigned");
        var cts = new CancellationTokenSource();
        fakeConsumer.OnMessagesExhausted = () => cts.Cancel();
        fakeConsumer.Enqueue(BuildJobAssignedJson(), key: "wrong-key");

        var consumer = new JobAssignedConsumer(
            "localhost:9092",
            _scopeFactoryMock.Object,
            NullLogger<JobAssignedConsumer>.Instance,
            _ => fakeConsumer);

        // Act
        await consumer.StartAsync(CancellationToken.None);
        await Task.Delay(200);
        await consumer.StopAsync(CancellationToken.None);

        // Assert
        _repositoryMock.Verify(r => r.ApplyAssignmentAsync(It.IsAny<JobAssignmentProjection>()), Times.Never);
        Assert.Single(fakeConsumer.CommittedOffsets);
    }

    #endregion

    #region JobStatusChangedConsumer Tests

    [Fact]
    public void JobStatusChangedConsumer_Deserialize_ValidJson_ReturnsEnvelope()
    {
        // Act
        var envelope = JobStatusChangedConsumer.Deserialize(BuildJobStatusChangedJson());

        // Assert
        Assert.NotNull(envelope);
        Assert.NotNull(envelope.Payload);
        Assert.Equal("COMPLETED", envelope.Payload.NewStatus);
    }

    [Theory]
    [InlineData(SampleJobId, true)]
    [InlineData("wrong-key", false)]
    public void JobStatusChangedConsumer_IsUsable_ValidatesMessageKey(string key, bool expectedUsable)
    {
        // Arrange
        var envelope = JobStatusChangedConsumer.Deserialize(BuildJobStatusChangedJson());

        // Act
        var usable = JobStatusChangedConsumer.IsUsable(envelope, key);

        // Assert
        Assert.Equal(expectedUsable, usable);
    }

    [Fact]
    public async Task JobStatusChangedConsumer_UsableMessage_CallsApplyJobStatusChangedAndCommits()
    {
        // Arrange
        var fakeConsumer = new FakeKafkaConsumer("job-status-changed");
        var cts = new CancellationTokenSource();
        fakeConsumer.OnMessagesExhausted = () => cts.Cancel();
        fakeConsumer.Enqueue(BuildJobStatusChangedJson(status: "COMPLETED"), key: SampleJobId);

        _repositoryMock
            .Setup(r => r.ApplyJobStatusChangedAsync(It.Is<JobStatusChangeProjection>(s =>
                s.JobId == SampleJobId &&
                s.NewStatus == "COMPLETED" &&
                s.OldStatus == "IN_PROGRESS")))
            .Returns(Task.CompletedTask);

        var consumer = new JobStatusChangedConsumer(
            "localhost:9092",
            _scopeFactoryMock.Object,
            NullLogger<JobStatusChangedConsumer>.Instance,
            _ => fakeConsumer);

        // Act
        await consumer.StartAsync(CancellationToken.None);
        await Task.Delay(200);
        await consumer.StopAsync(CancellationToken.None);

        // Assert
        _repositoryMock.Verify(r => r.ApplyJobStatusChangedAsync(It.IsAny<JobStatusChangeProjection>()), Times.Once);
        Assert.Single(fakeConsumer.CommittedOffsets);
    }

    [Fact]
    public async Task JobStatusChangedConsumer_UnusableMessage_DiscardsAndCommitsWithoutCallingRepository()
    {
        // Arrange (Key does not match payload jobId)
        var fakeConsumer = new FakeKafkaConsumer("job-status-changed");
        var cts = new CancellationTokenSource();
        fakeConsumer.OnMessagesExhausted = () => cts.Cancel();
        fakeConsumer.Enqueue(BuildJobStatusChangedJson(), key: "wrong-key");

        var consumer = new JobStatusChangedConsumer(
            "localhost:9092",
            _scopeFactoryMock.Object,
            NullLogger<JobStatusChangedConsumer>.Instance,
            _ => fakeConsumer);

        // Act
        await consumer.StartAsync(CancellationToken.None);
        await Task.Delay(200);
        await consumer.StopAsync(CancellationToken.None);

        // Assert
        _repositoryMock.Verify(r => r.ApplyJobStatusChangedAsync(It.IsAny<JobStatusChangeProjection>()), Times.Never);
        Assert.Single(fakeConsumer.CommittedOffsets);
    }

    #endregion
}
