using Kafka.Connect.Db2.Models;
using Xunit;

namespace UnitTests.Kafka.Connect.Db2.Models;

public class PluginConfigModelsTests
{
    [Fact]
    public void PluginConfig_DefaultSchema_IsDb2Inst1()
    {
        var config = new PluginConfig();
        Assert.Equal("DB2INST1", config.Schema);
    }

    [Fact]
    public void PluginConfig_DefaultPort_Is50000()
    {
        var config = new PluginConfig();
        Assert.Equal(50000, config.Port);
    }

    [Fact]
    public void PluginConfig_ConnectionString_ContainsDb2Credentials()
    {
        var config = new PluginConfig
        {
            Server = "myserver",
            Port = 50000,
            Database = "mydb",
            UserId = "admin",
            Password = "secret"
        };

        var cs = config.ConnectionString;

        Assert.Contains("Server=myserver:50000", cs);
        Assert.Contains("Database=mydb", cs);
        Assert.Contains("UID=admin", cs);
        Assert.Contains("PWD=secret", cs);
    }

    [Fact]
    public void PluginConfig_ConnectionString_WithMissingCredentials_DoesNotAddSqlServerProperties()
    {
        var config = new PluginConfig
        {
            Server = "myserver",
            Port = 50000,
            Database = "mydb"
        };

        var cs = config.ConnectionString;

        Assert.DoesNotContain("Integrated Security", cs);
        Assert.DoesNotContain("TrustServerCertificate", cs);
        Assert.Contains("UID=", cs);
        Assert.Contains("PWD=", cs);
    }
}
