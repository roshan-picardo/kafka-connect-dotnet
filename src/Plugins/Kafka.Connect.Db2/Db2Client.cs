using System.Data;
using IBM.Data.Db2;

namespace Kafka.Connect.Db2;

public interface IDb2Client
{
    string ApplicationName { get; }
    DB2Connection GetConnection();
}

public class Db2Client(string connector, DB2Connection connection) : IDb2Client, IDisposable
{
    private readonly DB2Connection _connection = EnsureOpen(connection);
    
    public string ApplicationName { get; } = connector;
    
    private static DB2Connection EnsureOpen(DB2Connection conn)
    {
        if (conn is not { State: ConnectionState.Open })
        {
            conn.Open();
        }
        return conn;
    }
    
    public DB2Connection GetConnection()
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
