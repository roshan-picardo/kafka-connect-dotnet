using System.Linq;
using Kafka.Connect.MongoDb;
using Kafka.Connect.Plugin;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace UnitTests.Kafka.Connect.MongoDb;

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
        Assert.Contains(services, s => s.ServiceType == typeof(IMongoCommandHandler));
        Assert.Contains(services, s => s.ServiceType == typeof(global::Kafka.Connect.MongoDb.Collections.IMongoClientProvider));
        Assert.Contains(services, s => s.ServiceType == typeof(global::Kafka.Connect.MongoDb.Collections.IMongoQueryRunner));
    }

    [Fact]
    public void AddServices_RegistersStrategies()
    {
        var services = new ServiceCollection();
        var initializer = new DefaultPluginInitializer();

        initializer.AddServices(services, new ConfigurationBuilder().Build());

        var strategyRegistrations = services.Count(s => s.ServiceType.Name == "IStrategy");
        Assert.True(strategyRegistrations >= 6);
    }
}
