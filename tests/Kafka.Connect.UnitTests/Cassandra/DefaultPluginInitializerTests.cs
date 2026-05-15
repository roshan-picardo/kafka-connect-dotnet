using System.Linq;
using Kafka.Connect.Cassandra;
using Kafka.Connect.Plugin;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace UnitTests.Kafka.Connect.Cassandra;

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
        Assert.Contains(services, s => s.ServiceType == typeof(ICassandraClientProvider));
        Assert.Contains(services, s => s.ServiceType == typeof(ICassandraSqlExecutor));
        Assert.Contains(services, s => s.ServiceType == typeof(ICassandraCommandHandler));
        Assert.Contains(services, s => s.ServiceType.Name == "IStrategySelector");
    }

    [Fact]
    public void AddServices_RegistersStrategies()
    {
        var services = new ServiceCollection();
        var initializer = new DefaultPluginInitializer();

        initializer.AddServices(services, new ConfigurationBuilder().Build());

        var strategyRegistrations = services.Count(s => s.ServiceType.Name == "IStrategy");
        Assert.True(strategyRegistrations >= 5);
    }
}
