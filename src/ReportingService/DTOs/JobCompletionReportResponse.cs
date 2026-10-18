namespace ReportingService.DTOs;

public sealed class JobCompletionReportResponse
{
    public List<CompletedJobItemResponse> Jobs { get; set; } = new();
    public int Total { get; set; }
}

public sealed class CompletedJobItemResponse
{
    public string JobId { get; set; } = string.Empty;
    public string JobReference { get; set; } = string.Empty;
    public string Region { get; set; } = string.Empty;
    public string? ServiceCategory { get; set; }
    public string? TechnicianId { get; set; }
    public string? TechnicianReference { get; set; }
    public DateTime CompletedAt { get; set; }
}
