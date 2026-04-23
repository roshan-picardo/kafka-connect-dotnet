using Amazon;
using Amazon.DynamoDBv2;
using Kafka.Connect.DynamoDb.Collections;
using Kafka.Connect.DynamoDb.Models;
using Kafka.Connect.Plugin.Providers;
using NSubstitute;
using Xunit;

using System;

namespace UnitTests.Kafka.Connect.DynamoDb.Collections;

public class DynamoDbClientProviderTests
{
    private readonly IConfigurationProvider _mockConfigProvider = Substitute.For<IConfigurationProvider>();

    [Fact]
    public void GetDynamoDbClient_WithValidConfig_ReturnsClient()
    {
        // Arrange
        var pluginConfig = new PluginConfig
        {
            Region = "us-east-1"
        };
        _mockConfigProvider.GetPluginConfig<PluginConfig>("connector1").Returns(pluginConfig);
        var provider = new DynamoDbClientProvider(_mockConfigProvider);

        // Act
        var client = provider.GetDynamoDbClient("connector1", 1);

        // Assert
        Assert.NotNull(client);
        Assert.IsAssignableFrom<IAmazonDynamoDB>(client);
    }

    [Fact]
    public void GetDynamoDbClient_SameConnectorAndTaskId_ReturnsCachedInstance()
    {
        // Arrange
        var pluginConfig = new PluginConfig { Region = "us-west-2" };
        _mockConfigProvider.GetPluginConfig<PluginConfig>("connector1").Returns(pluginConfig);
        var provider = new DynamoDbClientProvider(_mockConfigProvider);

        // Act
        var client1 = provider.GetDynamoDbClient("connector1", 1);
        var client2 = provider.GetDynamoDbClient("connector1", 1);

        // Assert - Should be same cached instance
        Assert.Same(client1, client2);
    }

    [Fact]
    public void GetDynamoDbClient_DifferentTaskIds_ReturnsDifferentInstances()
    {
        // Arrange
        var pluginConfig = new PluginConfig { Region = "us-west-2" };
        _mockConfigProvider.GetPluginConfig<PluginConfig>("connector1").Returns(pluginConfig);
        var provider = new DynamoDbClientProvider(_mockConfigProvider);

        // Act
        var client1 = provider.GetDynamoDbClient("connector1", 1);
        var client2 = provider.GetDynamoDbClient("connector1", 2);

        // Assert
        Assert.NotSame(client1, client2);
    }

    [Fact]
    public void GetDynamoDbClient_WithServiceUrl_CreatesClientWithCustomUrl()
    {
        // Arrange
        var pluginConfig = new PluginConfig
        {
            Region = "us-east-1",
            ServiceUrl = "http://localhost:8000"
        };
        _mockConfigProvider.GetPluginConfig<PluginConfig>("connector1").Returns(pluginConfig);
        var provider = new DynamoDbClientProvider(_mockConfigProvider);

        // Act
        var client = provider.GetDynamoDbClient("connector1", 1);

        // Assert
        Assert.NotNull(client);
    }

    [Fact]
    public void GetDynamoDbClient_WithCredentials_CreatesClientWithCredentials()
    {
        // Arrange
        var pluginConfig = new PluginConfig
        {
            Region = "us-east-1",
            AccessKeyId = "AKIAIOSFODNN7EXAMPLE",
            SecretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        };
        _mockConfigProvider.GetPluginConfig<PluginConfig>("connector1").Returns(pluginConfig);
        var provider = new DynamoDbClientProvider(_mockConfigProvider);

        // Act
        var client = provider.GetDynamoDbClient("connector1", 1);

        // Assert
        Assert.NotNull(client);
    }

    [Fact]
    public void GetDynamoDbClient_NoConfig_ThrowsInvalidOperationException()
    {
        // Arrange
        _mockConfigProvider.GetPluginConfig<PluginConfig>("nonexistent").Returns((PluginConfig)null);
        var provider = new DynamoDbClientProvider(_mockConfigProvider);

        // Act & Assert
        Assert.Throws<InvalidOperationException>(() => provider.GetDynamoDbClient("nonexistent", 1));
    }

    [Fact]
    public void GetStreamsClient_WithValidConfig_ReturnsClient()
    {
        // Arrange
        var pluginConfig = new PluginConfig { Region = "us-east-1" };
        _mockConfigProvider.GetPluginConfig<PluginConfig>("connector1").Returns(pluginConfig);
        var provider = new DynamoDbClientProvider(_mockConfigProvider);

        // Act
        var client = provider.GetStreamsClient("connector1", 1);

        // Assert
        Assert.NotNull(client);
    }

    [Fact]
    public void GetStreamsClient_SameConnectorAndTaskId_ReturnsCachedInstance()
    {
        // Arrange
        var pluginConfig = new PluginConfig { Region = "us-west-2" };
        _mockConfigProvider.GetPluginConfig<PluginConfig>("connector1").Returns(pluginConfig);
        var provider = new DynamoDbClientProvider(_mockConfigProvider);

        // Act
        var client1 = provider.GetStreamsClient("connector1", 1);
        var client2 = provider.GetStreamsClient("connector1", 1);

        // Assert - Should be same cached instance
        Assert.Same(client1, client2);
    }

    [Fact]
    public void GetStreamsClient_DifferentTaskIds_ReturnsDifferentInstances()
    {
        // Arrange
        var pluginConfig = new PluginConfig { Region = "us-west-2" };
        _mockConfigProvider.GetPluginConfig<PluginConfig>("connector1").Returns(pluginConfig);
        var provider = new DynamoDbClientProvider(_mockConfigProvider);

        // Act
        var client1 = provider.GetStreamsClient("connector1", 1);
        var client2 = provider.GetStreamsClient("connector1", 2);

        // Assert
        Assert.NotSame(client1, client2);
    }

    [Fact]
    public void GetStreamsClient_WithCredentials_CreatesClientWithCredentials()
    {
        // Arrange
        var pluginConfig = new PluginConfig
        {
            Region = "us-east-1",
            AccessKeyId = "AKIAIOSFODNN7EXAMPLE",
            SecretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        };
        _mockConfigProvider.GetPluginConfig<PluginConfig>("connector1").Returns(pluginConfig);
        var provider = new DynamoDbClientProvider(_mockConfigProvider);

        // Act
        var client = provider.GetStreamsClient("connector1", 1);

        // Assert
        Assert.NotNull(client);
    }
}
