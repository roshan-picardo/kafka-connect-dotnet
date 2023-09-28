using System.Data;
using Npgsql;

namespace Kafka.Connect.Postgres;

public interface IPostgresClient
{
    string ApplicationName { get;  }
    NpgsqlConnection GetConnection();
}

public class PostgresClient : IPostgresClient, IDisposable
{
    private readonly NpgsqlConnection _connection;

    public PostgresClient(string connector, NpgsqlConnection connection)
    {
        ApplicationName = connector;
        _connection = connection;
    }
    public string ApplicationName { get; }
    public NpgsqlConnection GetConnection()
    {
        if (_connection is { State: ConnectionState.Open })
        {
            return _connection;
        }
        _connection.Open();
        return _connection;
    }

    public void Dispose()
    {
        _connection?.Close();
        _connection?.Dispose();
    }
}