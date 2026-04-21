using System.Data;
using Oracle.ManagedDataAccess.Client;

namespace Kafka.Connect.Oracle;

public interface IOracleClient
{
    string ApplicationName { get; }
    OracleConnection GetConnection();
}

public class OracleClient(string connector, OracleConnection connection) : IOracleClient, IDisposable
{
    private readonly OracleConnection _connection = EnsureOpen(connection);
    
    public string ApplicationName { get; } = connector;
    
    private static OracleConnection EnsureOpen(OracleConnection conn)
    {
        if (conn is not { State: ConnectionState.Open })
        {
            conn.Open();
        }
        return conn;
    }
    
    public OracleConnection GetConnection()
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
