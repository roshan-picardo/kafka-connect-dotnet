using System;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using Kafka.Connect.DynamoDb.Strategies;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.DynamoDb.Strategies;

public class UpdateStrategyTests
{
    [Fact]
    public async void BuildModels_WithValidRecord_ReturnsUpdateWriteRequest()
    {
        // Arrange
        var logger = Substitute.For<ILogger<UpdateStrategy>>();
        var configProvider = Substitute.For<IConfigurationProvider>();
        var strategy = new UpdateStrategy(logger, configProvider);

        var record = new ConnectRecord("topic", 0, 0)
        {
            Serialized = new ConnectMessage<byte[]> { Key = [1, 2, 3] },
            Deserialized = new ConnectMessage<JsonNode>
            {
                Value = JsonNode.Parse("{\"id\": \"123\", \"name\": \"Jane\", \"age\": 31}")
            }
        };

        // Act
        var result = await strategy.Build<WriteRequest>("connector1", record);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(Status.Updating, result.Status);
        Assert.NotNull(result.Model.PutRequest);
        Assert.NotEmpty(result.Model.PutRequest.Item);
    }

    [Fact]
    public async void BuildModels_WithNestedJsonObject_ConvertsCorrectly()
    {
        // Arrange
        var logger = Substitute.For<ILogger<UpdateStrategy>>();
        var configProvider = Substitute.For<IConfigurationProvider>();
        var strategy = new UpdateStrategy(logger, configProvider);

        var record = new ConnectRecord("topic", 0, 0)
        {
            Serialized = new ConnectMessage<byte[]> { Key = [1, 2, 3] },
            Deserialized = new ConnectMessage<JsonNode>
            {
                Value = JsonNode.Parse(@"
                {
                    ""id"": ""123"",
                    ""profile"": {
                        ""firstName"": ""Jane"",
                        ""lastName"": ""Doe""
                    },
                    ""status"": ""active""
                }")
            }
        };

        // Act
        var result = await strategy.Build<WriteRequest>("connector1", record);

        // Assert
        Assert.NotNull(result.Model.PutRequest.Item);
        Assert.True(result.Model.PutRequest.Item.Count >= 2);
    }

    [Fact]
    public async Task BuildModels_WithCommandRecord_ThrowsNotImplementedException()
    {
        // Arrange
        var logger = Substitute.For<ILogger<UpdateStrategy>>();
        var configProvider = Substitute.For<IConfigurationProvider>();
        var strategy = new UpdateStrategy(logger, configProvider);
        var command = new CommandRecord();

        // Act & Assert
        await Assert.ThrowsAsync<NotImplementedException>(async () =>
            await strategy.Build<WriteRequest>("connector1", command));
    }
}
