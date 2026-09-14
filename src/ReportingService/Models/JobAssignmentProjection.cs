namespace ReportingService.Models;

/// <summary>Assignment data absorbed from a Dispatch-owned JobAssigned event.</summary>
public sealed class JobAssignmentProjection
{
    public string JobId { get; init; } = string.Empty;

    public string TechnicianId { get; init; } = string.Empty;

    public string TechnicianReference { get; init; } = string.Empty;

    public DateTime AssignedAt { get; init; }
}
