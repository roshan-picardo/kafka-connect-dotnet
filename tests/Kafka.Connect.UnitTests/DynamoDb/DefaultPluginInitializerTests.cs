using Kafka.Connect.DynamoDb;
using Kafka.Connect.DynamoDb.Collections;
using Kafka.Connect.Plugin;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

using System.Linq;

namespace UnitTests.Kafka.Connect.DynamoDb;

public class DefaultPluginInitializerTests
{
    [Fact]
    public void AddServices_RegistersAllDynamoDbDependencies()
    {
        // Arrange
        var services = new ServiceCollection();
        var config = new ConfigurationBuilder().Build();
        var initializer = new DefaultPluginInitializer();

        // Act
        initializer.AddServices(services, config, ("connector1", 1));

        // Assert
        Assert.Contains(services, s => s.ServiceType == typeof(IDynamoDbCommandHandler));
        Assert.Contains(services, s => s.ServiceType == typeof(IPluginHandler));
        Assert.Contains(services, s => s.ServiceType == typeof(IPluginInitializer));
        Assert.Contains(services, s => s.ServiceType == typeof(IDynamoDbClientProvider));
        Assert.Contains(services, s => s.ServiceType == typeof(IDynamoDbQueryRunner));
    }

    [Fact]
    public void AddServices_RegistersAllStrategies()
    {
        // Arrange
        var services = new ServiceCollection();
        var config = new ConfigurationBuilder().Build();
        var initializer = new DefaultPluginInitializer();

        // Act
        initializer.AddServices(services, config);

        // Assert
        var provider = services.BuildServiceProvider();
        
        // Get all strategies registered
        var strategiesFromServiceCollection = services.Where(s => s.ServiceType?.Name?.Contains("Strategy") == true).ToList();
        
        Assert.True(strategiesFromServiceCollection.Count >= 6, 
            $"Expected at least 6 strategies registered, found {strategiesFromServiceCollection.Count}");
    }

    [Fact]
    public void AddServices_WithMultipleConnectors_RegistersSuccessfully()
    {
        // Arrange
        var services = new ServiceCollection();
        var config = new ConfigurationBuilder().Build();
        var initializer = new DefaultPluginInitializer();

        // Act
        var connectors = new[]
        {
            ("connector1", 2),
            ("connector2", 3),
            ("connector3", 1)
        };
        initializer.AddServices(services, config, connectors);

        // Assert - Should complete without throwing
        Assert.True(true);
    }

    [Fact]
    public void AddServices_NoConnectors_StillRegistersServices()
    {
        // Arrange
        var services = new ServiceCollection();
        var config = new ConfigurationBuilder().Build();
        var initializer = new DefaultPluginInitializer();

        // Act
        initializer.AddServices(services, config);

        // Assert
        Assert.Contains(services, s => s.ServiceType == typeof(IPluginInitializer));
    }

    [Fact]
    public void AddServices_ClientProviderAsSingleton_SameInstanceReturned()
    {
        // Arrange
        var services = new ServiceCollection();
        var config = new ConfigurationBuilder().Build();
        var initializer = new DefaultPluginInitializer();

        // Act
        initializer.AddServices(services, config);

        // Assert - registration should be singleton
        var registration = services.SingleOrDefault(s => s.ServiceType == typeof(IDynamoDbClientProvider));
        Assert.NotNull(registration);
        Assert.Equal(ServiceLifetime.Singleton, registration!.Lifetime);
    }
}
