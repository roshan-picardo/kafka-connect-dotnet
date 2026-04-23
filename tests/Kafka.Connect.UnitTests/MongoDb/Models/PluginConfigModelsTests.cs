using System;
using System.Collections.Generic;
using Kafka.Connect.MongoDb.Models;
using Xunit;

namespace UnitTests.Kafka.Connect.MongoDb.Models;

public class PluginConfigModelsTests
{
    [Fact]
    public void ConnectionUri_EncodesPasswordFromBase64()
    {
        var config = new PluginConfig
        {
            ConnectionUri = "mongodb://{0}:{1}@host:27017/{2}",
            Username = "user",
            Password = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes("p@ss w:rd")),
            Database = "db1"
        };

        var uri = config.ConnectionUri;

        Assert.Contains("mongodb://user:", uri);
        Assert.Contains("@host:27017/db1", uri);
        Assert.Contains("p%40ss+w%3ard", uri);
    }

    [Fact]
    public void ConnectionUri_WhenNull_ReturnsNull()
    {
        var config = new PluginConfig();
        Assert.Null(config.ConnectionUri);
    }

    [Fact]
    public void Defaults_AreExpected()
    {
        var config = new PluginConfig();
        Assert.True(config.IsWriteOrdered);
    }

    [Fact]
    public void CommandConfig_ToJson_ContainsProperties()
    {
        var command = new CommandConfig
        {
            Collection = "users",
            Timestamp = 123,
            Filters = new Dictionary<string, object> { ["id"] = 7 },
            Keys = ["id"]
        };

        var json = command.ToJson();

        Assert.Equal("users", json!["Collection"]!.GetValue<string>());
        Assert.Equal(123, json["Timestamp"]!.GetValue<long>());
    }
}
