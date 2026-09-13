namespace ReportingService.DTOs;

public sealed class JobsByTechnicianResponse
{
    public List<TechnicianJobCountResponse> Technicians { get; init; } = [];
    public int Total { get; init; }
}

public sealed class TechnicianJobCountResponse
{
    public string TechnicianId { get; init; } = string.Empty;
    public string TechnicianReference { get; init; } = string.Empty;
    public int JobCount { get; init; }
}
