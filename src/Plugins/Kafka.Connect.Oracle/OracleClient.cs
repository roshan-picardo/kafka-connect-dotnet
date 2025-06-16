using System.Data;
using Oracle.ManagedDataAccess.Client;

namespace Kafka.Connect.Oracle;

public interface IOracleClient
{
    string ApplicationName { get; }
    OracleConnection GetConnection();
}

public class OracleClient : IOracleClient, IDisposable
{
    private readonly OracleConnection _connection;

    public OracleClient(string connector, OracleConnection connection)
    {
        ApplicationName = connector;
        _connection = connection;
        if (_connection is not { State: ConnectionState.Open })
        {
            _connection.Open();
        }
    }
    
    public string ApplicationName { get; }
    
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
