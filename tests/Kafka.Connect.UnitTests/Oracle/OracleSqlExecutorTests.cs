using System;
using System.Threading.Tasks;
using Kafka.Connect.Oracle;
using Oracle.ManagedDataAccess.Client;
using Xunit;

namespace UnitTests.Kafka.Connect.Oracle;

public class OracleSqlExecutorTests
{
    [Fact]
    public async Task ExecuteScalarAsync_WithDisconnectedConnection_Throws()
    {
        var sut = new OracleSqlExecutor();
        await Assert.ThrowsAnyAsync<Exception>(() => sut.ExecuteScalarAsync(new OracleConnection(), "SELECT 1 FROM DUAL"));
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_WithDisconnectedConnection_Throws()
    {
        var sut = new OracleSqlExecutor();
        await Assert.ThrowsAnyAsync<Exception>(() => sut.ExecuteNonQueryAsync(new OracleConnection(), "DELETE FROM x"));
    }

    [Fact]
    public async Task QuerySingleColumnAsync_WithDisconnectedConnection_Throws()
    {
        var sut = new OracleSqlExecutor();
        await Assert.ThrowsAnyAsync<Exception>(() => sut.QuerySingleColumnAsync(new OracleConnection(), "SELECT x FROM y"));
    }

    [Fact]
    public async Task QueryRowsAsync_WithDisconnectedConnection_Throws()
    {
        var sut = new OracleSqlExecutor();
        await Assert.ThrowsAnyAsync<Exception>(() => sut.QueryRowsAsync(new OracleConnection(), "SELECT x FROM y"));
    }
}
