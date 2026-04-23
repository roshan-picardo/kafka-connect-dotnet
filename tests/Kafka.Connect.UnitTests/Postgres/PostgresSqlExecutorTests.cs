using System;
using System.Threading.Tasks;
using Kafka.Connect.Postgres;
using Npgsql;
using Xunit;

namespace UnitTests.Kafka.Connect.Postgres;

public class PostgresSqlExecutorTests
{
    [Fact]
    public async Task ExecuteScalarAsync_WithDisconnectedConnection_Throws()
    {
        var sut = new PostgresSqlExecutor();
        await Assert.ThrowsAnyAsync<Exception>(() => sut.ExecuteScalarAsync(new NpgsqlConnection(), "SELECT 1"));
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_WithDisconnectedConnection_Throws()
    {
        var sut = new PostgresSqlExecutor();
        await Assert.ThrowsAnyAsync<Exception>(() => sut.ExecuteNonQueryAsync(new NpgsqlConnection(), "DELETE FROM x"));
    }

    [Fact]
    public async Task QueryRowsAsync_WithDisconnectedConnection_Throws()
    {
        var sut = new PostgresSqlExecutor();
        await Assert.ThrowsAnyAsync<Exception>(() => sut.QueryRowsAsync(new NpgsqlConnection(), "SELECT 1"));
    }
}