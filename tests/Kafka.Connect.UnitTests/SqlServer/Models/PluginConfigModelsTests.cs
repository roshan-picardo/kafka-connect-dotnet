using Kafka.Connect.SqlServer.Models;
using Xunit;

namespace UnitTests.Kafka.Connect.SqlServer.Models;

public class PluginConfigModelsTests
{
    [Fact]
    public void PluginConfig_DefaultSchema_IsDbo()
    {
        var config = new PluginConfig();
        Assert.Equal("dbo", config.Schema);
    }

    [Fact]
    public void PluginConfig_DefaultPort_Is1433()
    {
        var config = new PluginConfig();
        Assert.Equal(1433, config.Port);
    }

    [Fact]
    public void PluginConfig_ConnectionString_WithSqlAuth_ContainsCredentials()
    {
        var config = new PluginConfig
        {
            Server = "myserver",
            Port = 1433,
            Database = "mydb",
            UserId = "admin",
            Password = "secret",
            IntegratedSecurity = false,
            TrustServerCertificate = true
        };

        var cs = config.ConnectionString;

        Assert.Contains("Server=myserver,1433", cs);
        Assert.Contains("Database=mydb", cs);
        Assert.Contains("User Id=admin", cs);
        Assert.Contains("Password=secret", cs);
        Assert.Contains("TrustServerCertificate=True", cs);
    }

    [Fact]
    public void PluginConfig_ConnectionString_WithIntegratedSecurity_ContainsIntegratedSecurity()
    {
        var config = new PluginConfig
        {
            Server = "myserver",
            Port = 1433,
            Database = "mydb",
            IntegratedSecurity = true,
            TrustServerCertificate = true
        };

        var cs = config.ConnectionString;

        Assert.Contains("Integrated Security=True", cs);
        Assert.DoesNotContain("User Id", cs);
        Assert.DoesNotContain("Password", cs);
    }
}
