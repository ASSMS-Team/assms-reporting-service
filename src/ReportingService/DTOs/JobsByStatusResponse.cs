namespace ReportingService.DTOs;

// The jobs-by-status report. An object wrapping the array rather than the bare
// array, because the total has to travel with it: the caller cannot work the
// total out from the rows without assuming the rows are complete, and once a
// later story pages or caps them that assumption stops holding.
public class JobsByStatusResponse
{
    /// <summary>One entry per status that at least one job in range carries. Ordered by status. Empty when no jobs match.</summary>
    public IReadOnlyList<JobStatusCountResponse> Statuses { get; set; } = Array.Empty<JobStatusCountResponse>();

    /// <summary>How many jobs the report counted in total - the sum of the counts above, not the number of statuses.</summary>
    public int Total { get; set; }
}
