using System;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using Kafka.Connect.DynamoDb.Models;
using Kafka.Connect.DynamoDb.Strategies;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.DynamoDb.Strategies;

public class DeleteStrategyTests
{
    [Fact]
    public async void BuildModels_WithValidRecord_ReturnsDeleteWriteRequest()
    {
        // Arrange
        var logger = Substitute.For<ILogger<DeleteStrategy>>();
        var configProvider = Substitute.For<IConfigurationProvider>();

        var pluginConfig = new PluginConfig { Filter = "id={id}" };
        configProvider.GetPluginConfig<PluginConfig>("connector1").Returns(pluginConfig);

        var strategy = new DeleteStrategy(logger, configProvider);

        var record = new ConnectRecord("topic", 0, 0)
        {
            Serialized = new ConnectMessage<byte[]> { Key = [1, 2, 3] },
            Deserialized = new ConnectMessage<JsonNode>
            {
                Value = JsonNode.Parse("{\"id\": \"123\", \"name\": \"John\"}")
            }
        };

        // Act
        var result = await strategy.Build<WriteRequest>("connector1", record);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(Status.Deleting, result.Status);
        Assert.NotNull(result.Model.DeleteRequest);
        Assert.NotEmpty(result.Model.DeleteRequest.Key);
    }

    [Fact]
    public async void BuildModels_WithCompositeKey_BuildsCompleteKey()
    {
        // Arrange
        var logger = Substitute.For<ILogger<DeleteStrategy>>();
        var configProvider = Substitute.For<IConfigurationProvider>();

        var pluginConfig = new PluginConfig { Filter = "pk={id},sk={timestamp}" };
        configProvider.GetPluginConfig<PluginConfig>("connector1").Returns(pluginConfig);

        var strategy = new DeleteStrategy(logger, configProvider);

        var record = new ConnectRecord("topic", 0, 0)
        {
            Serialized = new ConnectMessage<byte[]> { Key = [1, 2, 3] },
            Deserialized = new ConnectMessage<JsonNode>
            {
                Value = JsonNode.Parse("{\"id\": \"123\", \"timestamp\": \"2023-01-01\", \"name\": \"John\"}")
            }
        };

        // Act
        var result = await strategy.Build<WriteRequest>("connector1", record);

        // Assert
        Assert.NotNull(result.Model.DeleteRequest.Key);
        Assert.True(result.Model.DeleteRequest.Key.Count >= 1);
    }

    [Fact]
    public async void BuildModels_WithoutFilter_ReturnsEmptyKey()
    {
        // Arrange
        var logger = Substitute.For<ILogger<DeleteStrategy>>();
        var configProvider = Substitute.For<IConfigurationProvider>();

        var pluginConfig = new PluginConfig { Filter = null };
        configProvider.GetPluginConfig<PluginConfig>("connector1").Returns(pluginConfig);

        var strategy = new DeleteStrategy(logger, configProvider);

        var record = new ConnectRecord("topic", 0, 0)
        {
            Serialized = new ConnectMessage<byte[]> { Key = [1, 2, 3] },
            Deserialized = new ConnectMessage<JsonNode>
            {
                Value = JsonNode.Parse("{\"id\": \"123\"}")
            }
        };

        // Act
        var result = await strategy.Build<WriteRequest>("connector1", record);

        // Assert
        Assert.NotNull(result.Model.DeleteRequest.Key);
        Assert.Empty(result.Model.DeleteRequest.Key);
    }

    [Fact]
    public async Task BuildModels_WithCommandRecord_ThrowsNotImplementedException()
    {
        // Arrange
        var logger = Substitute.For<ILogger<DeleteStrategy>>();
        var configProvider = Substitute.For<IConfigurationProvider>();
        var pluginConfig = new PluginConfig { Filter = "id={id}" };
        configProvider.GetPluginConfig<PluginConfig>("connector1").Returns(pluginConfig);

        var strategy = new DeleteStrategy(logger, configProvider);
        var command = new CommandRecord();

        // Act & Assert
        await Assert.ThrowsAsync<NotImplementedException>(async () =>
            await strategy.Build<WriteRequest>("connector1", command));
    }
}
