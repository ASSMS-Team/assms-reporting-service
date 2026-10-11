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
                -- A redelivered JobCreated must not move an advanced job back to CREATED.
                status           = IF(" + StatusRank("incoming.status") + @" > " + StatusRank("job_projection.status") + @",
                                      incoming.status, job_projection.status),
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

    public async Task ApplyAssignmentAsync(JobAssignmentProjection assignment)
    {
        await using var connection = _connectionFactory.CreateConnection();
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = @"
            INSERT INTO job_assignment_projection
                (job_id, technician_id, technician_reference, assigned_at)
            VALUES
                (@jobId, @technicianId, @technicianReference, @assignedAt)
            AS incoming
            ON DUPLICATE KEY UPDATE
                technician_id = incoming.technician_id,
                technician_reference = incoming.technician_reference,
                assigned_at = incoming.assigned_at;";
        command.Parameters.AddWithValue("@jobId", assignment.JobId);
        command.Parameters.AddWithValue("@technicianId", assignment.TechnicianId);
        command.Parameters.AddWithValue("@technicianReference", assignment.TechnicianReference);
        command.Parameters.AddWithValue("@assignedAt", assignment.AssignedAt.ToUniversalTime());

        await command.ExecuteNonQueryAsync();

        // The assignment also moves the job to ASSIGNED for jobs-by-status, but
        // never back from a later status a lifecycle event already applied.
        await using var statusCommand = connection.CreateCommand();
        statusCommand.CommandText = @"
            UPDATE job_projection
            SET status = 'ASSIGNED'
            WHERE job_id = @jobId
              AND " + StatusRank("status") + @" < " + StatusRank("'ASSIGNED'") + @";";
        statusCommand.Parameters.AddWithValue("@jobId", assignment.JobId);
        await statusCommand.ExecuteNonQueryAsync();
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

    public async Task<IReadOnlyList<TechnicianJobCount>> GetTechnicianJobCountsAsync(DateTime? from, DateTime? to, string? region)
    {
        await using var connection = _connectionFactory.CreateConnection();
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();

        var conditions = new List<string>();
        if (from.HasValue)
        {
            // A lower bound includes an assignment at exactly that instant.
            conditions.Add("a.assigned_at >= @from");
            command.Parameters.AddWithValue("@from", from.Value);
        }
        if (to.HasValue)
        {
            // An upper bound excludes its instant. Adjacent report windows
            // therefore do not count the same assignment twice.
            conditions.Add("a.assigned_at < @to");
            command.Parameters.AddWithValue("@to", to.Value);
        }
        if (region is not null)
        {
            conditions.Add("j.region = @region");
            command.Parameters.AddWithValue("@region", region);
        }

        command.CommandText = @"
            SELECT a.technician_id, a.technician_reference, COUNT(*) AS job_count
            FROM job_projection j
            JOIN job_assignment_projection a ON a.job_id = j.job_id" +
            (conditions.Count == 0 ? string.Empty : "\n            WHERE " + string.Join(" AND ", conditions)) + @"
            GROUP BY a.technician_id, a.technician_reference
            ORDER BY a.technician_reference;";

        var counts = new List<TechnicianJobCount>();
        await using var reader = (MySqlDataReader)await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            counts.Add(new TechnicianJobCount
            {
                TechnicianId = Convert.ToString(reader.GetValue(0))!,
                TechnicianReference = reader.GetString(1),
                JobCount = (int)reader.GetInt64(2),
            });
        }

        return counts;
    }

    public async Task ApplyJobStatusChangedAsync(JobStatusChangeProjection change)
    {
        await using var connection = _connectionFactory.CreateConnection();
        await connection.OpenAsync();

        await using var statusCommand = connection.CreateCommand();
        // Forward transitions only: a redelivered or out-of-order event (for
        // example an old ASSIGNED -> IN_PROGRESS after COMPLETED) is ignored.
        statusCommand.CommandText = @"
            UPDATE job_projection
            SET status = @newStatus
            WHERE job_id = @jobId
              AND " + StatusRank("status") + @" < " + StatusRank("@newStatus") + @";";
        statusCommand.Parameters.AddWithValue("@jobId", change.JobId);
        statusCommand.Parameters.AddWithValue("@newStatus", change.NewStatus);
        await statusCommand.ExecuteNonQueryAsync();

        if (string.Equals(change.NewStatus, "COMPLETED", StringComparison.OrdinalIgnoreCase))
        {
            await using var completionCommand = connection.CreateCommand();
            completionCommand.CommandText = @"
                INSERT INTO job_completion_projection
                    (job_id, job_reference, technician_id, technician_reference, completed_at)
                VALUES
                    (@jobId, @jobReference, @technicianId, @technicianReference, @completedAt)
                AS incoming
                ON DUPLICATE KEY UPDATE
                    job_reference = incoming.job_reference,
                    technician_id = incoming.technician_id,
                    technician_reference = incoming.technician_reference,
                    completed_at = incoming.completed_at;";
            completionCommand.Parameters.AddWithValue("@jobId", change.JobId);
            completionCommand.Parameters.AddWithValue("@jobReference", change.JobReference);
            completionCommand.Parameters.AddWithValue("@technicianId", (object?)change.TechnicianId ?? DBNull.Value);
            completionCommand.Parameters.AddWithValue("@technicianReference", (object?)change.TechnicianReference ?? DBNull.Value);
            completionCommand.Parameters.AddWithValue("@completedAt", change.OccurredAt.ToUniversalTime());

            await completionCommand.ExecuteNonQueryAsync();
        }
    }

    public async Task<IReadOnlyList<JobCompletionRecord>> GetJobCompletionsAsync(DateTime? from, DateTime? to, string? region)
    {
        await using var connection = _connectionFactory.CreateConnection();
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();

        var conditions = new List<string>();
        if (from.HasValue)
        {
            conditions.Add("c.completed_at >= @from");
            command.Parameters.AddWithValue("@from", from.Value);
        }
        if (to.HasValue)
        {
            conditions.Add("c.completed_at < @to");
            command.Parameters.AddWithValue("@to", to.Value);
        }
        if (region is not null)
        {
            conditions.Add("j.region = @region");
            command.Parameters.AddWithValue("@region", region);
        }

        command.CommandText = @"
            SELECT c.job_id, c.job_reference, COALESCE(j.region, '') AS region, j.service_category, c.technician_id, c.technician_reference, c.completed_at
            FROM job_completion_projection c
            LEFT JOIN job_projection j ON j.job_id = c.job_id" +
            (conditions.Count == 0 ? string.Empty : "\n            WHERE " + string.Join(" AND ", conditions)) + @"
            ORDER BY c.completed_at DESC, c.job_reference ASC;";

        var records = new List<JobCompletionRecord>();
        await using var reader = (MySqlDataReader)await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            records.Add(new JobCompletionRecord
            {
                // CHAR(36) columns are read back as Guid by MySqlConnector, so
                // GetString throws; go through the boxed value instead.
                JobId = Convert.ToString(reader.GetValue(0))!,
                JobReference = reader.GetString(1),
                Region = reader.GetString(2),
                ServiceCategory = reader.IsDBNull(3) ? null : reader.GetString(3),
                TechnicianId = reader.IsDBNull(4) ? null : Convert.ToString(reader.GetValue(4)),
                TechnicianReference = reader.IsDBNull(5) ? null : reader.GetString(5),
                // Stored as UTC; mark it so the API serializes it with a Z.
                CompletedAt = DateTime.SpecifyKind(reader.GetDateTime(6), DateTimeKind.Utc),
            });
        }

        return records;
    }

    // Lifecycle order used to keep the read model moving forward only.
    // COMPLETED and CANCELLED are terminal; an unknown status ranks lowest.
    private static string StatusRank(string expression) => $@"(CASE UPPER({expression})
                WHEN 'CREATED' THEN 0
                WHEN 'ASSIGNED' THEN 1
                WHEN 'IN_PROGRESS' THEN 2
                WHEN 'COMPLETED' THEN 3
                WHEN 'CANCELLED' THEN 3
                ELSE -1 END)";
}
