namespace ReportingService.Models;

/// <summary>One technician group in the Jobs-by-Technician reporting read model.</summary>
public sealed class TechnicianJobCount
{
    public string TechnicianId { get; init; } = string.Empty;

    public string TechnicianReference { get; init; } = string.Empty;

    public int JobCount { get; init; }
}
