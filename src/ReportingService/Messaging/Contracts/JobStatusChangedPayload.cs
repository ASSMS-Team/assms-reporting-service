namespace ReportingService.Messaging.Contracts;

/// <summary>The Job-service-owned status change fact projected by Reporting.</summary>
public sealed class JobStatusChangedPayload
{
    public const string Topic = "job-status-changed";
    public const string ConsumerGroup = "assms-reporting-job-status-changed";
    public const string EventType = "JobStatusChanged";
    public const int EventVersion = 1;
    public const string Producer = "job-service";

    public string JobId { get; set; } = string.Empty;
    public string JobReference { get; set; } = string.Empty;
    public string? AssignmentId { get; set; }
    public string? TechnicianId { get; set; }
    public string? TechnicianReference { get; set; }
    public string OldStatus { get; set; } = string.Empty;
    public string NewStatus { get; set; } = string.Empty;
    public DateTime OccurredAt { get; set; }
}
