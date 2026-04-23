using System;
using System.Threading.Tasks;
using Kafka.Connect.MariaDb;
using MySqlConnector;
using Xunit;

namespace UnitTests.Kafka.Connect.MariaDb;

public class MariaDbSqlExecutorTests
{
    [Fact]
    public async Task ExecuteScalarAsync_WithDisconnectedConnection_Throws()
    {
        var sut = new MariaDbSqlExecutor();
        await Assert.ThrowsAnyAsync<Exception>(() => sut.ExecuteScalarAsync(new MySqlConnection(), "SELECT 1"));
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_WithDisconnectedConnection_Throws()
    {
        var sut = new MariaDbSqlExecutor();
        await Assert.ThrowsAnyAsync<Exception>(() => sut.ExecuteNonQueryAsync(new MySqlConnection(), "DELETE FROM x"));
    }

    [Fact]
    public async Task QuerySingleColumnAsync_WithDisconnectedConnection_Throws()
    {
        var sut = new MariaDbSqlExecutor();
        await Assert.ThrowsAnyAsync<Exception>(() => sut.QuerySingleColumnAsync(new MySqlConnection(), "SELECT x FROM y"));
    }

    [Fact]
    public async Task QueryRowsAsync_WithDisconnectedConnection_Throws()
    {
        var sut = new MariaDbSqlExecutor();
        await Assert.ThrowsAnyAsync<Exception>(() => sut.QueryRowsAsync(new MySqlConnection(), "SELECT x FROM y"));
    }
}
