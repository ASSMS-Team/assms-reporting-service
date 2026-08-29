namespace ReportingService.Models;

// One row of the job_projection table: a job as this service holds it for
// reporting, built from a JobCreated event rather than from anything this
// service was told directly.
//
// projected_at is not here. The database writes it on insert and refreshes it
// on update, and nothing in the service reads it back, so carrying it would
// only create a second place where it could be set.
public class JobProjection
{
    // The Job Service's id, reused as this table's primary key - see the
    // migration for why that is what makes the upsert idempotent.
    public string JobId { get; set; } = string.Empty;

    public string JobReference { get; set; } = string.Empty;

    // Not constrained to a known set anywhere in this service. Whatever the Job
    // Service published is what is stored.
    public string Status { get; set; } = string.Empty;

    public string Region { get; set; } = string.Empty;

    public string Priority { get; set; } = string.Empty;

    public string ServiceCategory { get; set; } = string.Empty;

    // When the job was created upstream, not when this row was written.
    public DateTime JobCreatedAt { get; set; }
}
