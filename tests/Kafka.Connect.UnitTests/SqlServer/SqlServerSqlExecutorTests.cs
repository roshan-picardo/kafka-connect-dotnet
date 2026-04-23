using System;
using System.Threading.Tasks;
using Kafka.Connect.SqlServer;
using Microsoft.Data.SqlClient;
using Xunit;

namespace UnitTests.Kafka.Connect.SqlServer;

public class SqlServerSqlExecutorTests
{
    [Fact]
    public async Task ExecuteScalarAsync_WithDisconnectedConnection_Throws()
    {
        var sut = new SqlServerSqlExecutor();
        await Assert.ThrowsAnyAsync<Exception>(() => sut.ExecuteScalarAsync(new SqlConnection(), "SELECT 1"));
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_WithDisconnectedConnection_Throws()
    {
        var sut = new SqlServerSqlExecutor();
        await Assert.ThrowsAnyAsync<Exception>(() => sut.ExecuteNonQueryAsync(new SqlConnection(), "DELETE FROM x"));
    }

    [Fact]
    public async Task QueryRowsAsync_WithDisconnectedConnection_Throws()
    {
        var sut = new SqlServerSqlExecutor();
        await Assert.ThrowsAnyAsync<Exception>(() => sut.QueryRowsAsync(new SqlConnection(), "SELECT x FROM y"));
    }
}
