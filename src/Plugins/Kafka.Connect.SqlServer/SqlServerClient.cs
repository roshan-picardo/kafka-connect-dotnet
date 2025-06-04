using System.Data;
using Microsoft.Data.SqlClient;

namespace Kafka.Connect.SqlServer;

public interface ISqlServerClient
{
    string ApplicationName { get; }
    SqlConnection GetConnection();
}

public class SqlServerClient : ISqlServerClient, IDisposable
{
    private readonly SqlConnection _connection;

    public SqlServerClient(string connector, SqlConnection connection)
    {
        ApplicationName = connector;
        _connection = connection;
        if (_connection is not { State: ConnectionState.Open })
        {
            _connection.Open();
        }
    }
    
    public string ApplicationName { get; }
    
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
