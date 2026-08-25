using MySqlConnector;

namespace ReportingService.Repositories;

public interface IDbConnectionFactory
{
    MySqlConnection CreateConnection();
}
