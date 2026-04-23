using Kafka.Connect.Postgres;
using Npgsql;
using Xunit;

namespace UnitTests.Kafka.Connect.Postgres;

public class PostgresClientTests
{
    [Fact]
    public void Constructor_WithInvalidConnection_ThrowsNpgsqlException()
    {
        var connection = new NpgsqlConnection("Host=127.0.0.1;Port=1;Database=db;Username=u;Password=p;Timeout=1");

        Assert.ThrowsAny<NpgsqlException>(() => new PostgresClient("c1", connection));
    }
}