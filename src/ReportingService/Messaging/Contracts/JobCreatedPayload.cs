namespace ReportingService.Messaging.Contracts;

// The JobCreated payload as this service reads it. Published by the Job Service
// to job-created; see the coupling note on EventEnvelope for why this is a
// redefinition rather than a shared type.
//
// Only the seven fields the projection stores are declared. The contract also
// defines customerId, assetId and problemDescription; they are on the wire and
// are deliberately not named here, because a report of job counts has no use
// for them and a consumer that names a field has to keep up with it. Unknown
// JSON properties are ignored on deserialization, so leaving them out costs
// nothing at read time.
public class JobCreatedPayload
{
    public const string Topic = "job-created";

    // The consumer group this service reads job-created with, named by the
    // contract document's assms-<consuming-service>-<topic> convention. Its own
    // group, never shared with Dispatch: a shared group would make Kafka hand
    // each event to only one of the two, and both need every event.
    public const string ConsumerGroup = "assms-reporting-job-created";

    public const string EventType = "JobCreated";

    public string JobId { get; set; } = string.Empty;

    public string JobReference { get; set; } = string.Empty;

    public string ServiceCategory { get; set; } = string.Empty;

    public string Priority { get; set; } = string.Empty;

    public string Region { get; set; } = string.Empty;

    // Always CREATED on this event, but read from the message rather than
    // assumed, so the projection carries what was published.
    public string Status { get; set; } = string.Empty;

    // When the job row was created in jobdb. This becomes job_created_at - the
    // business timestamp the report filters on.
    public DateTime CreatedAt { get; set; }
}
