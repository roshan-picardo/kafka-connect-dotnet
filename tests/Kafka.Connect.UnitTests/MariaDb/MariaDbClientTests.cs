using Kafka.Connect.MariaDb;
using MySqlConnector;
using Xunit;

namespace UnitTests.Kafka.Connect.MariaDb;

public class MariaDbClientTests
{
    [Fact]
    public void Constructor_WithInvalidConnection_ThrowsMySqlException()
    {
        // Use an unreachable local endpoint so Open() fails fast.
        var connection = new MySqlConnection("Server=127.0.0.1;Port=1;Database=db;User Id=u;Password=p;Connection Timeout=1;");

        Assert.Throws<MySqlException>(() => new MariaDbClient("c1", connection));
    }
}
