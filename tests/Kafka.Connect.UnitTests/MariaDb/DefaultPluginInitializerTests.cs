using System.Linq;
using Kafka.Connect.MariaDb;
using Kafka.Connect.Plugin;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace UnitTests.Kafka.Connect.MariaDb;

public class DefaultPluginInitializerTests
{
    [Fact]
    public void AddServices_RegistersExpectedServiceTypes()
    {
        var services = new ServiceCollection();
        var initializer = new DefaultPluginInitializer();

        initializer.AddServices(services, new ConfigurationBuilder().Build(), ("c1", 1));

        Assert.Contains(services, s => s.ServiceType == typeof(IPluginHandler));
        Assert.Contains(services, s => s.ServiceType == typeof(IPluginInitializer));
        Assert.Contains(services, s => s.ServiceType == typeof(IMariaDbClientProvider));
        Assert.Contains(services, s => s.ServiceType == typeof(IMariaDbSqlExecutor));
        Assert.Contains(services, s => s.ServiceType == typeof(IMariaDbCommandHandler));
    }

    [Fact]
    public void AddServices_RegistersMultipleStrategiesAndSelector()
    {
        var services = new ServiceCollection();
        var initializer = new DefaultPluginInitializer();

        initializer.AddServices(services, new ConfigurationBuilder().Build());

        var strategyRegistrations = services.Count(s => s.ServiceType.Name == "IStrategy");
        Assert.True(strategyRegistrations >= 5);
        Assert.Contains(services, s => s.ServiceType.Name == "IStrategySelector");
    }

    [Fact]
    public void AddServices_ClientProvider_IsSingleton()
    {
        var services = new ServiceCollection();
        var initializer = new DefaultPluginInitializer();

        initializer.AddServices(services, new ConfigurationBuilder().Build());

        var reg = services.Single(s => s.ServiceType == typeof(IMariaDbClientProvider));
        Assert.Equal(ServiceLifetime.Singleton, reg.Lifetime);
    }
}
