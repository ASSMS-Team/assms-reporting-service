namespace ReportingService.Models;

// One group of the jobs-by-status report: a status and how many projected jobs
// carry it. Statuses come out of the data, not out of a list this service
// keeps, so a status no job has is simply absent rather than reported as zero.
public class JobStatusCount
{
    public string Status { get; set; } = string.Empty;

    public int Count { get; set; }
}
