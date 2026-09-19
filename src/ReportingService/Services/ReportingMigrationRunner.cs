using ReportingService.Repositories;

namespace ReportingService.Services;

/// <summary>Applies packaged Reporting schema migrations once and in filename order.</summary>
public sealed class ReportingMigrationRunner
{
    private readonly IDbConnectionFactory _connectionFactory;
    public ReportingMigrationRunner(IDbConnectionFactory connectionFactory) => _connectionFactory = connectionFactory;

    public async Task ApplyAsync(CancellationToken cancellationToken = default)
    {
        var directory = Path.Combine(AppContext.BaseDirectory, "migrations");
        var files = Directory.GetFiles(directory, "V*__*.sql").OrderBy(Path.GetFileName, StringComparer.Ordinal).ToArray();
        await using var connection = _connectionFactory.CreateConnection();
        await connection.OpenAsync(cancellationToken);
        await using var history = connection.CreateCommand();
        history.CommandText = "CREATE TABLE IF NOT EXISTS schema_migrations (filename VARCHAR(255) NOT NULL PRIMARY KEY, applied_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6));";
        await history.ExecuteNonQueryAsync(cancellationToken);
        foreach (var file in files)
        {
            var name = Path.GetFileName(file);
            await using var check = connection.CreateCommand();
            check.CommandText = "SELECT COUNT(*) FROM schema_migrations WHERE filename = @name;";
            check.Parameters.AddWithValue("@name", name);
            if (Convert.ToInt32(await check.ExecuteScalarAsync(cancellationToken)) > 0) continue;
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
            try
            {
                await using var apply = connection.CreateCommand();
                apply.Transaction = transaction;
                apply.CommandText = await File.ReadAllTextAsync(file, cancellationToken);
                await apply.ExecuteNonQueryAsync(cancellationToken);
                await using var record = connection.CreateCommand();
                record.Transaction = transaction;
                record.CommandText = "INSERT INTO schema_migrations (filename) VALUES (@name);";
                record.Parameters.AddWithValue("@name", name);
                await record.ExecuteNonQueryAsync(cancellationToken);
                await transaction.CommitAsync(cancellationToken);
            }
            catch { await transaction.RollbackAsync(cancellationToken); throw; }
        }
    }
}
