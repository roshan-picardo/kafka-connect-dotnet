using System.Linq;
using Kafka.Connect.Plugin;
using Kafka.Connect.Postgres;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace UnitTests.Kafka.Connect.Postgres;

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
        Assert.Contains(services, s => s.ServiceType == typeof(IPostgresClientProvider));
        Assert.Contains(services, s => s.ServiceType == typeof(IPostgresSqlExecutor));
        Assert.Contains(services, s => s.ServiceType == typeof(IPostgresCommandHandler));
        Assert.Contains(services, s => s.ServiceType.Name == "IStrategySelector");
    }

    [Fact]
    public void AddServices_RegistersStrategies()
    {
        var services = new ServiceCollection();
        var initializer = new DefaultPluginInitializer();

        initializer.AddServices(services, new ConfigurationBuilder().Build());

        Assert.True(services.Count(s => s.ServiceType.Name == "IStrategy") >= 5);
    }

    [Fact]
    public void AddServices_ClientProvider_IsSingleton()
    {
        var services = new ServiceCollection();
        var initializer = new DefaultPluginInitializer();

        initializer.AddServices(services, new ConfigurationBuilder().Build());

        var registration = services.Single(s => s.ServiceType == typeof(IPostgresClientProvider));
        Assert.Equal(ServiceLifetime.Singleton, registration.Lifetime);
    }
}