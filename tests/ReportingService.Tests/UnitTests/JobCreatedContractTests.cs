using System.Text.Json;

using ReportingService.Messaging.Consumers;
using ReportingService.Messaging.Contracts;

namespace ReportingService.Tests;

// The contract types in this service are a redefinition of what the Job Service
// publishes, not a reference to its types. That duplication is deliberate - see
// docs/read-model/job-projection.md - and these tests are the price of it: they
// pin the redefined types against a real published message, so drift shows up
// here rather than as a report that quietly counts nothing.
//
// The message below is the example from the contract document itself
// (assms-platform-infrastructure/docs/kafka/event-contracts.md), copied
// verbatim. If that document changes, this string is what should change first.
public class JobCreatedContractTests
{
    private const string ContractExampleMessage = """
        {
          "eventId": "3d2a1b90-6c77-4f18-b0a1-5c9e7d4a2f31",
          "eventType": "JobCreated",
          "eventVersion": 1,
          "occurredAt": "2026-08-29T09:14:32.118Z",
          "producer": "job-service",
          "payload": {
            "jobId": "9f1c7a24-8f4e-4c3a-9a52-2b6d0f5e1a77",
            "jobReference": "JOB-7K2M9X",
            "customerId": "c41b9e2d-77a3-4d5f-8e10-6b2c9a0f4d13",
            "assetId": "a70e5c18-2d94-4b6a-9f37-1e8d5c3b7a62",
            "serviceCategory": "REPAIR",
            "problemDescription": "Air conditioner in the server room is not cooling and trips the breaker after ten minutes.",
            "priority": "HIGH",
            "region": "WESTERN",
            "status": "CREATED",
            "createdAt": "2026-08-29T09:14:32.118Z"
          }
        }
        """;

    [Fact]
    public void Deserialize_ReadsEveryFieldTheProjectionStores()
    {
        // Act
        var payload = JobCreatedConsumer.Deserialize(ContractExampleMessage);

        // Assert - all seven, because every one of them is a column.
        Assert.NotNull(payload);
        Assert.Equal("9f1c7a24-8f4e-4c3a-9a52-2b6d0f5e1a77", payload!.JobId);
        Assert.Equal("JOB-7K2M9X", payload.JobReference);
        Assert.Equal("REPAIR", payload.ServiceCategory);
        Assert.Equal("HIGH", payload.Priority);
        Assert.Equal("WESTERN", payload.Region);
        Assert.Equal("CREATED", payload.Status);
    }

    [Fact]
    public void Deserialize_ReadsCreatedAtAsUtc()
    {
        // Act
        var payload = JobCreatedConsumer.Deserialize(ContractExampleMessage);

        // Assert - the trailing Z has to survive into the DateTime's Kind. Read
        // as Local, this value would be written to job_created_at shifted by the
        // consumer machine's timezone, and every date-filtered report would be
        // wrong by that offset.
        Assert.NotNull(payload);
        Assert.Equal(DateTimeKind.Utc, payload!.CreatedAt.Kind);
        Assert.Equal(new DateTime(2026, 8, 29, 9, 14, 32, 118), payload.CreatedAt);
    }

    [Fact]
    public void Deserialize_IgnoresPayloadFieldsThisServiceDoesNotDeclare()
    {
        // Arrange - customerId, assetId and problemDescription are in the
        // contract and in the message above, and are deliberately not declared
        // on JobCreatedPayload.
        //
        // Act
        var payload = JobCreatedConsumer.Deserialize(ContractExampleMessage);

        // Assert - reading the message succeeds anyway. This is the property the
        // contract requires of a consumer, and it is what lets the Job Service
        // add an optional field without waiting for this service to be released.
        Assert.NotNull(payload);
        Assert.Equal("JOB-7K2M9X", payload!.JobReference);
    }

    [Fact]
    public void Deserialize_ToleratesAFieldAddedAfterThisServiceWasWritten()
    {
        // Arrange - an optional field added upstream, which under the contract's
        // versioning rules does not raise eventVersion and so arrives with no
        // warning.
        const string withNewField = """
            {
              "eventId": "3d2a1b90-6c77-4f18-b0a1-5c9e7d4a2f31",
              "eventType": "JobCreated",
              "eventVersion": 1,
              "occurredAt": "2026-08-29T09:14:32.118Z",
              "producer": "job-service",
              "payload": {
                "jobId": "9f1c7a24-8f4e-4c3a-9a52-2b6d0f5e1a77",
                "jobReference": "JOB-7K2M9X",
                "serviceCategory": "REPAIR",
                "priority": "HIGH",
                "region": "WESTERN",
                "status": "CREATED",
                "createdAt": "2026-08-29T09:14:32.118Z",
                "scheduledDate": "2026-09-04"
              }
            }
            """;

        // Act
        var payload = JobCreatedConsumer.Deserialize(withNewField);

        // Assert
        Assert.NotNull(payload);
        Assert.Equal("9f1c7a24-8f4e-4c3a-9a52-2b6d0f5e1a77", payload!.JobId);
    }

    [Fact]
    public void Deserialize_ReadsTheEnvelopeAsWellAsThePayload()
    {
        // Arrange - the envelope is shared across all three ASSMS topics, so it
        // is worth pinning independently of the payload it happens to carry.
        //
        // Act
        var envelope = JsonSerializer.Deserialize<EventEnvelope<JobCreatedPayload>>(
            ContractExampleMessage,
            new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

        // Assert
        Assert.NotNull(envelope);
        Assert.Equal("3d2a1b90-6c77-4f18-b0a1-5c9e7d4a2f31", envelope!.EventId);
        Assert.Equal("JobCreated", envelope.EventType);
        Assert.Equal(1, envelope.EventVersion);
        Assert.Equal("job-service", envelope.Producer);
        Assert.Equal(DateTimeKind.Utc, envelope.OccurredAt.Kind);
    }

    [Fact]
    public void Deserialize_OfABareNull_ReturnsNull()
    {
        // A tombstone or a literal null on the topic. Valid JSON, but not an
        // event - the consumer treats it as malformed rather than dereferencing
        // it.
        Assert.Null(JobCreatedConsumer.Deserialize("null"));
    }

    [Fact]
    public void Deserialize_OfAnEnvelopeWithNoPayload_ReturnsNull()
    {
        // Arrange - the five envelope fields with nothing inside them. No retry
        // will make a payload appear, so this has to be distinguishable from a
        // successful read.
        const string noPayload = """
            {
              "eventId": "3d2a1b90-6c77-4f18-b0a1-5c9e7d4a2f31",
              "eventType": "JobCreated",
              "eventVersion": 1,
              "occurredAt": "2026-08-29T09:14:32.118Z",
              "producer": "job-service"
            }
            """;

        Assert.Null(JobCreatedConsumer.Deserialize(noPayload));
    }

    [Theory]
    [InlineData("not json at all")]
    [InlineData("{ \"eventType\": ")]
    [InlineData("")]
    public void Deserialize_OfMalformedJson_Throws(string value)
    {
        // The consumer catches exactly this to decide a message is poison and
        // commit past it, so the exception type is behaviour rather than an
        // implementation detail.
        Assert.Throws<JsonException>(() => JobCreatedConsumer.Deserialize(value));
    }

    [Fact]
    public void Topic_AndConsumerGroup_MatchTheContractDocument()
    {
        // The group name follows the document's assms-<consuming-service>-<topic>
        // convention. Getting it wrong is not a build failure and not a runtime
        // error - it silently makes this service a different reader of the topic,
        // which is why it is asserted rather than left to review.
        Assert.Equal("job-created", JobCreatedPayload.Topic);
        Assert.Equal("assms-reporting-job-created", JobCreatedPayload.ConsumerGroup);
        Assert.Equal("JobCreated", JobCreatedPayload.EventType);
    }
}
