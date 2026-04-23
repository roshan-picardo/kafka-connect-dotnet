using Kafka.Connect.MySql;
using MySql.Data.MySqlClient;
using Xunit;

namespace UnitTests.Kafka.Connect.MySql;

public class MySqlClientTests
{
    [Fact]
    public void Constructor_WithInvalidConnection_ThrowsMySqlException()
    {
        var connection = new MySqlConnection("Server=127.0.0.1;Port=1;Database=db;Uid=u;Pwd=p;Connection Timeout=1;");

        Assert.Throws<MySqlException>(() => new MySqlClient("c1", connection));
    }
}
