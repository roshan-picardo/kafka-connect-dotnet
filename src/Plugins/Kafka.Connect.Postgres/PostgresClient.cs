using System.Data;
using Npgsql;

namespace Kafka.Connect.Postgres;

public interface IPostgresClient
{
    string ApplicationName { get;  }
    NpgsqlConnection GetConnection();
}

public class PostgresClient(string connector, NpgsqlConnection connection) : IPostgresClient, IDisposable
{
    private readonly NpgsqlConnection _connection = EnsureOpen(connection);
    
    public string ApplicationName { get; } = connector;
    
    private static NpgsqlConnection EnsureOpen(NpgsqlConnection conn)
    {
        if (conn is not { State: ConnectionState.Open })
        {
            conn.Open();
        }
        return conn;
    }
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