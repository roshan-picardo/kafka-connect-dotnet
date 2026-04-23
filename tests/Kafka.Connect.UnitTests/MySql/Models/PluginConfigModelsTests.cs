using Kafka.Connect.MySql.Models;
using Xunit;

namespace UnitTests.Kafka.Connect.MySql.Models;

public class PluginConfigModelsTests
{
    [Fact]
    public void PluginConfig_ConnectionString_UsesConfiguredValues()
    {
        var config = new PluginConfig
        {
            Host = "db.local",
            Port = 3307,
            Database = "app",
            UserId = "user",
            Password = "pass"
        };

        var connectionString = config.ConnectionString;

        Assert.Contains("Server=db.local", connectionString);
        Assert.Contains("Port=3307", connectionString);
        Assert.Contains("Database=app", connectionString);
        Assert.Contains("Uid=user", connectionString);
        Assert.Contains("Pwd=pass", connectionString);
    }

    [Fact]
    public void Defaults_AreInitialized()
    {
        var plugin = new PluginConfig();
        var command = new CommandConfig();
        var changelog = new ChangelogConfig();
        var snapshot = new SnapshotConfig();

        Assert.Equal("mysql", plugin.Schema);
        Assert.Equal("mysql", command.Schema);
        Assert.Equal("mysql", changelog.Schema);
        Assert.Equal(3306, plugin.Port);
        Assert.Equal(1, changelog.Retention);
        Assert.False(snapshot.Enabled);
    }
}
