using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using Kafka.Connect.DynamoDb.Models;
using Kafka.Connect.DynamoDb.Strategies;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.DynamoDb.Strategies;

public class ReadStrategyTests
{
    [Fact]
    public async void BuildModels_WithCommandRecord_ReturnsScanModel()
    {
        // Arrange
        var logger = Substitute.For<ILogger<ReadStrategy>>();
        var strategy = new ReadStrategy(logger);

        var command = new CommandRecord
        {
            BatchSize = 100
        };
        var commandConfig = new CommandConfig
        {
            TableName = "users",
            Filters = new Dictionary<string, object>()
        };
        command.Command = JsonSerializer.SerializeToNode(commandConfig);

        // Act
        var result = await strategy.Build<ScanModel>("connector1", command);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(Status.Selecting, result.Status);
        Assert.NotNull(result.Model);
        Assert.Equal("SCAN", result.Model.Operation);
        Assert.Equal("users", result.Model.Request.TableName);
        Assert.Equal(100, result.Model.Request.Limit);
    }

    [Fact]
    public async void BuildModels_WithFilters_AppliesFilterExpression()
    {
        // Arrange
        var logger = Substitute.For<ILogger<ReadStrategy>>();
        var strategy = new ReadStrategy(logger);

        var command = new CommandRecord { BatchSize = 50 };
        var commandConfig = new CommandConfig
        {
            TableName = "products",
            Filters = new Dictionary<string, object> { ["price"] = 100 }
        };
        command.Command = JsonSerializer.SerializeToNode(commandConfig);

        // Act
        var result = await strategy.Build<ScanModel>("connector1", command);

        // Assert
        Assert.NotNull(result.Model);
        Assert.NotEmpty(result.Model.Request.FilterExpression);
        Assert.NotNull(result.Model.Request.ExpressionAttributeNames);
        Assert.NotNull(result.Model.Request.ExpressionAttributeValues);
    }

    [Fact]
    public async Task BuildModels_WithConnectRecord_ThrowsNotImplementedException()
    {
        // Arrange
        var logger = Substitute.For<ILogger<ReadStrategy>>();
        var strategy = new ReadStrategy(logger);
        var record = new ConnectRecord("topic", 0, 0);

        // Act & Assert
        await Assert.ThrowsAsync<NotImplementedException>(async () =>
            await strategy.Build<ScanModel>("connector1", record));
    }
}
