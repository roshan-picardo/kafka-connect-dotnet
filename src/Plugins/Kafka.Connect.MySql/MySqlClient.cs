using System.Data;
using MySql.Data.MySqlClient;

namespace Kafka.Connect.MySql;

public interface IMySqlClient
{
    string ApplicationName { get; }
    MySqlConnection GetConnection();
}

public class MySqlClient(string connector, MySqlConnection connection) : IMySqlClient, IDisposable
{
    private readonly MySqlConnection _connection = EnsureOpen(connection);
    
    public string ApplicationName { get; } = connector;
    
    private static MySqlConnection EnsureOpen(MySqlConnection conn)
    {
        if (conn is not { State: ConnectionState.Open })
        {
            conn.Open();
        }
        return conn;
    }
    
    public MySqlConnection GetConnection()
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
