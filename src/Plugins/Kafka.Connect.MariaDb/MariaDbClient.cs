using System.Data;
using MySqlConnector;

namespace Kafka.Connect.MariaDb;

public interface IMariaDbClient
{
    string ApplicationName { get; }
    MySqlConnection GetConnection();
}

public class MariaDbClient : IMariaDbClient, IDisposable
{
    private readonly MySqlConnection _connection;

    public MariaDbClient(string connector, MySqlConnection connection)
    {
        ApplicationName = connector;
        _connection = connection;
        if (_connection is not { State: ConnectionState.Open })
        {
            _connection.Open();
        }
    }
    
    public string ApplicationName { get; }
    
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
