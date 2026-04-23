using System.Collections.Generic;
using System.Text.Json.Serialization;
using Kafka.Connect.Oracle.Models;
using Xunit;

namespace UnitTests.Kafka.Connect.Oracle.Models;

public class PluginConfigModelsTests
{
    [Fact]
    public void PluginConfig_DefaultSchema_IsSystemForOracle()
    {
        var sut = new PluginConfig();

        Assert.Equal("SYSTEM", sut.Schema);
    }

    [Fact]
    public void PluginConfig_Properties_CanBeModified()
    {
        var config = new PluginConfig
        {
            Host = "localhost",
            UserId = "test",
            Database = "testdb"
        };

        Assert.Equal("localhost", config.Host);
        Assert.Equal("test", config.UserId);
        Assert.Equal("testdb", config.Database);
    }
}
