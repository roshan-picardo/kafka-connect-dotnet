using System.Collections.Generic;
using System.Text.Json;
using Kafka.Connect.MariaDb.Models;
using Xunit;

namespace UnitTests.Kafka.Connect.MariaDb.Models;

public class PluginConfigModelsTests
{
    [Fact]
    public void PluginConfig_ConnectionString_ComposesFromProperties()
    {
        var config = new PluginConfig
        {
            Host = "localhost",
            Port = 3307,
            Database = "appdb",
            UserId = "user",
            Password = "pass"
        };

        Assert.Equal("Server=localhost;Port=3307;Database=appdb;User Id=user;Password=pass;", config.ConnectionString);
    }

    [Fact]
    public void PluginConfig_Defaults_AreExpected()
    {
        var config = new PluginConfig();

        Assert.Equal(3306, config.Port);
        Assert.Equal("mariadb", config.Schema);
    }

    [Fact]
    public void CommandConfig_SnapshotFlags_WorkAsExpected()
    {
        var command = new CommandConfig
        {
            Snapshot = new SnapshotConfig { Enabled = true, Total = 0 }
        };

        Assert.True(command.IsSnapshot());
        Assert.True(command.IsInitial());
    }

    [Fact]
    public void CommandConfig_ToJson_SerializesShape()
    {
        var command = new CommandConfig
        {
            Table = "users",
            Schema = "dbo",
            Keys = ["id"],
            Filters = new Dictionary<string, object> { ["id"] = 5 }
        };

        var json = command.ToJson();

        Assert.Equal("users", json!["Table"]!.GetValue<string>());
        Assert.Equal("dbo", json["Schema"]!.GetValue<string>());
    }

    [Fact]
    public void ChangelogAndSnapshot_Defaults_AreExpected()
    {
        var changelog = new ChangelogConfig();
        var snapshot = new SnapshotConfig();

        Assert.Equal("mariadb", changelog.Schema);
        Assert.Equal(1, changelog.Retention);
        Assert.False(snapshot.Enabled);
        Assert.Equal(0, snapshot.Total);
    }
}
