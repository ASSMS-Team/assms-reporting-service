namespace ReportingService.DTOs;

// One status and how many jobs hold it.
public class JobStatusCountResponse
{
    /// <summary>Lifecycle status of the jobs counted, as the Job Service published it - for example CREATED.</summary>
    public string Status { get; set; } = string.Empty;

    /// <summary>How many jobs in range carry this status. Always one or more; a status no job holds is absent rather than zero.</summary>
    public int Count { get; set; }
}
