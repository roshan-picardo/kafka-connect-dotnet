using System;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using Kafka.Connect.DynamoDb.Strategies;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.DynamoDb.Strategies;

public class InsertStrategyTests
{
    [Fact]
    public async void BuildModels_WithValidRecord_ReturnsInsertWriteRequest()
    {
        // Arrange
        var logger = Substitute.For<ILogger<InsertStrategy>>();
        var strategy = new InsertStrategy(logger);

        var record = new ConnectRecord("topic", 0, 0)
        {
            Serialized = new ConnectMessage<byte[]> { Key = [1, 2, 3] },
            Deserialized = new ConnectMessage<JsonNode>
            {
                Key = JsonNode.Parse("{\"id\": \"123\"}"),
                Value = JsonNode.Parse("{\"id\": \"123\", \"name\": \"John\", \"age\": 30}")
            }
        };

        // Act
        var result = await strategy.Build<WriteRequest>("connector1", record);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(Status.Inserting, result.Status);
        Assert.NotNull(result.Model);
        Assert.NotNull(result.Model.PutRequest);
        Assert.NotEmpty(result.Model.PutRequest.Item);
    }

    [Fact]
    public async void BuildModels_WithComplexJson_ConvertsProperly()
    {
        // Arrange
        var logger = Substitute.For<ILogger<InsertStrategy>>();
        var strategy = new InsertStrategy(logger);

        var record = new ConnectRecord("topic", 0, 0)
        {
            Serialized = new ConnectMessage<byte[]> { Key = [1, 2, 3] },
            Deserialized = new ConnectMessage<JsonNode>
            {
                Value = JsonNode.Parse(@"
                {
                    ""id"": ""123"",
                    ""name"": ""John"",
                    ""age"": 30,
                    ""active"": true,
                    ""metadata"": {
                        ""created"": ""2023-01-01""
                    }
                }")
            }
        };

        // Act
        var result = await strategy.Build<WriteRequest>("connector1", record);

        // Assert
        Assert.NotNull(result.Model.PutRequest);
        Assert.True(result.Model.PutRequest.Item.Count >= 3);
    }

    [Fact]
    public async Task BuildModels_WithCommandRecord_ThrowsNotImplementedException()
    {
        // Arrange
        var logger = Substitute.For<ILogger<InsertStrategy>>();
        var strategy = new InsertStrategy(logger);
        var command = new CommandRecord();

        // Act & Assert
        await Assert.ThrowsAsync<NotImplementedException>(async () =>
            await strategy.Build<WriteRequest>("connector1", command));
    }
}
