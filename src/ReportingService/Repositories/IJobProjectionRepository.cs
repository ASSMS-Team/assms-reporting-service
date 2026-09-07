using ReportingService.Models;

namespace ReportingService.Repositories;

public interface IJobProjectionRepository
{
    // Inserts the job, or overwrites the row already held under this job_id.
    // There is no separate "does it exist" call and no exception to catch on a
    // repeat: the primary key settles it inside one statement, which is what
    // lets the consumer treat a redelivered event exactly like a first
    // delivery.
    Task UpsertAsync(JobProjection projection);

    // The jobs-by-status report. Both bounds are optional and independent:
    // supplying neither counts every projected job, supplying both counts the
    // jobs created between them inclusive, and supplying one bounds that end
    // alone. Bounds are compared against job_created_at - when the job was
    // raised upstream - not against when this service absorbed it.
    //
    // A status with no jobs in range does not come back as a zero row; it does
    // not come back at all, because the grouping is over the rows that exist.
    Task<IReadOnlyList<JobStatusCount>> GetStatusCountsAsync(DateTime? from, DateTime? to);
}
