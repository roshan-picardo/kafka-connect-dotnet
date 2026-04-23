using System;
using System.Threading.Tasks;
using Kafka.Connect.Db2;
using IBM.Data.Db2;
using Xunit;

namespace UnitTests.Kafka.Connect.Db2;

public class Db2SqlExecutorTests
{
    [Fact]
    public async Task ExecuteScalarAsync_WithDisconnectedConnection_Throws()
    {
        var sut = new Db2SqlExecutor();
        await Assert.ThrowsAnyAsync<Exception>(() => sut.ExecuteScalarAsync(new DB2Connection(), "SELECT 1"));
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_WithDisconnectedConnection_Throws()
    {
        var sut = new Db2SqlExecutor();
        await Assert.ThrowsAnyAsync<Exception>(() => sut.ExecuteNonQueryAsync(new DB2Connection(), "DELETE FROM x"));
    }

    [Fact]
    public async Task QueryRowsAsync_WithDisconnectedConnection_Throws()
    {
        var sut = new Db2SqlExecutor();
        await Assert.ThrowsAnyAsync<Exception>(() => sut.QueryRowsAsync(new DB2Connection(), "SELECT x FROM y"));
    }
}
