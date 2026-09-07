using MySqlConnector;

using ReportingService.Models;

namespace ReportingService.Repositories;

public class JobProjectionRepository : IJobProjectionRepository
{
    private readonly IDbConnectionFactory _connectionFactory;

    public JobProjectionRepository(IDbConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }

    // INSERT ... ON DUPLICATE KEY UPDATE rather than a SELECT followed by an
    // INSERT or an UPDATE: the two-statement form has a race between the check
    // and the write, and two consumer instances - or one consumer replaying a
    // partition - would both see "not there" and both insert. One statement
    // resolves it in the engine, against the primary key.
    //
    // projected_at is not in either list. Its column default writes it on the
    // insert and its ON UPDATE refreshes it here, so the time this row was
    // absorbed is recorded by the database in both directions.
    //
    // The row alias is the current form of this clause; the older VALUES(col)
    // spelling has been deprecated since MySQL 8.0.20, and the local server is
    // 8.0.40.
    public async Task UpsertAsync(JobProjection projection)
    {
        await using var connection = _connectionFactory.CreateConnection();
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = @"
            INSERT INTO job_projection
                (job_id, job_reference, status, region, priority, service_category, job_created_at)
            VALUES
                (@jobId, @jobReference, @status, @region, @priority, @serviceCategory, @jobCreatedAt)
            AS incoming
            ON DUPLICATE KEY UPDATE
                job_reference    = incoming.job_reference,
                status           = incoming.status,
                region           = incoming.region,
                priority         = incoming.priority,
                service_category = incoming.service_category,
                job_created_at   = incoming.job_created_at;";

        command.Parameters.AddWithValue("@jobId", projection.JobId);
        command.Parameters.AddWithValue("@jobReference", projection.JobReference);
        command.Parameters.AddWithValue("@status", projection.Status);
        command.Parameters.AddWithValue("@region", projection.Region);
        command.Parameters.AddWithValue("@priority", projection.Priority);
        command.Parameters.AddWithValue("@serviceCategory", projection.ServiceCategory);
        command.Parameters.AddWithValue("@jobCreatedAt", projection.JobCreatedAt);

        await command.ExecuteNonQueryAsync();
    }

    public async Task<IReadOnlyList<JobStatusCount>> GetStatusCountsAsync(DateTime? from, DateTime? to)
    {
        await using var connection = _connectionFactory.CreateConnection();
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();

        // The two bounds are appended independently rather than as a single
        // BETWEEN, because either one may be absent. With both supplied the
        // pair is exactly BETWEEN @from AND @to - inclusive at each end - and
        // with neither the WHERE clause is omitted entirely rather than filled
        // with a range wide enough to match everything.
        var conditions = new List<string>();

        if (from.HasValue)
        {
            conditions.Add("job_created_at >= @from");
            command.Parameters.AddWithValue("@from", from.Value);
        }

        if (to.HasValue)
        {
            conditions.Add("job_created_at <= @to");
            command.Parameters.AddWithValue("@to", to.Value);
        }

        var whereClause = conditions.Count == 0
            ? string.Empty
            : "\n            WHERE " + string.Join(" AND ", conditions);

        // ORDER BY status so the report reads the same way twice. Without it
        // MySQL is free to return the groups in any order, and a table that
        // reshuffles between refreshes is hard to read.
        command.CommandText = @"
            SELECT status, COUNT(*) AS job_count
            FROM job_projection" + whereClause + @"
            GROUP BY status
            ORDER BY status;";

        await using var reader = (MySqlDataReader)await command.ExecuteReaderAsync();

        var statusOrdinal = reader.GetOrdinal("status");
        var countOrdinal = reader.GetOrdinal("job_count");

        var counts = new List<JobStatusCount>();

        while (await reader.ReadAsync())
        {
            counts.Add(new JobStatusCount
            {
                Status = reader.GetString(statusOrdinal),
                // COUNT(*) comes back as a BIGINT. The narrowing is safe for
                // any job volume this system will hold.
                Count = (int)reader.GetInt64(countOrdinal)
            });
        }

        return counts;
    }
}
