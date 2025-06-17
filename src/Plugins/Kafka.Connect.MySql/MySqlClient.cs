using System.Data;
using MySql.Data.MySqlClient;

namespace Kafka.Connect.MySql;

public interface IMySqlClient
{
    string ApplicationName { get; }
    MySqlConnection GetConnection();
}

public class MySqlClient : IMySqlClient, IDisposable
{
    private readonly MySqlConnection _connection;

    public MySqlClient(string connector, MySqlConnection connection)
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
