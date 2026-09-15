namespace ReportingService.Messaging.Contracts;

/// <summary>The Dispatch-owned assignment fact projected by Reporting.</summary>
public sealed class JobAssignedPayload
{
    public const string Topic = "job-assigned";
    public const string ConsumerGroup = "assms-reporting-job-assigned";
    public const string EventType = "JobAssigned";
    public const int EventVersion = 1;
    public const string Producer = "dispatch-service";

    public string AssignmentId { get; set; } = string.Empty;
    public string JobId { get; set; } = string.Empty;
    public string JobReference { get; set; } = string.Empty;
    public string TechnicianId { get; set; } = string.Empty;
    public string TechnicianReference { get; set; } = string.Empty;
    public DateTime AssignedAt { get; set; }
}
