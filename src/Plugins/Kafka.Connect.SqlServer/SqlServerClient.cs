using System.Data;
using Microsoft.Data.SqlClient;

namespace Kafka.Connect.SqlServer;

public interface ISqlServerClient
{
    string ApplicationName { get; }
    SqlConnection GetConnection();
}

public class SqlServerClient(string connector, SqlConnection connection) : ISqlServerClient, IDisposable
{
    private readonly SqlConnection _connection = EnsureOpen(connection);
    
    public string ApplicationName { get; } = connector;
    
    private static SqlConnection EnsureOpen(SqlConnection conn)
    {
        if (conn is not { State: ConnectionState.Open })
        {
            conn.Open();
        }
        return conn;
    }
    
    public SqlConnection GetConnection()
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
