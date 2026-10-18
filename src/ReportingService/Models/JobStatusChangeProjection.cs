namespace ReportingService.Models;

public sealed class JobStatusChangeProjection
{
    public string JobId { get; set; } = string.Empty;
    public string JobReference { get; set; } = string.Empty;
    public string? TechnicianId { get; set; }
    public string? TechnicianReference { get; set; }
    public string OldStatus { get; set; } = string.Empty;
    public string NewStatus { get; set; } = string.Empty;
    public DateTime OccurredAt { get; set; }
}
