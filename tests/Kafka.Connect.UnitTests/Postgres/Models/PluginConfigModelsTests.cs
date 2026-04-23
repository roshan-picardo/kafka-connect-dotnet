using Kafka.Connect.Postgres.Models;
using Xunit;

namespace UnitTests.Kafka.Connect.Postgres.Models;

public class PluginConfigModelsTests
{
    [Fact]
    public void PluginConfig_DefaultSchema_IsPublic()
    {
        var sut = new PluginConfig();

        Assert.Equal("public", sut.Schema);
        Assert.Equal(5432, sut.Port);
    }

    [Fact]
    public void PluginConfig_ConnectionString_ComposesExpectedValues()
    {
        var sut = new PluginConfig
        {
            Host = "localhost",
            Port = 5433,
            UserId = "postgres",
            Password = "secret",
            Database = "appdb"
        };

        Assert.Equal("Host=localhost;Port=5433;User Id=postgres;Password='secret';Database=appdb", sut.ConnectionString);
    }
}