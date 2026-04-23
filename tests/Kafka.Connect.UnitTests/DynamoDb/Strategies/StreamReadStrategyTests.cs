using System;
using System.Threading.Tasks;
using Kafka.Connect.DynamoDb.Models;
using Kafka.Connect.DynamoDb.Strategies;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using NSubstitute;
using Xunit;

using System.Text.Json;
using System.Collections.Generic;

namespace UnitTests.Kafka.Connect.DynamoDb.Strategies;

public class StreamReadStrategyTests
{
    [Fact]
    public async void BuildModels_WithCommandRecord_ReturnsStreamModel()
    {
        // Arrange
        var logger = Substitute.For<ILogger<StreamReadStrategy>>();
        var strategy = new StreamReadStrategy(logger);
        
        var command = new CommandRecord { BatchSize = 100 };
        var commandConfig = new CommandConfig
        {
            TableName = "users",
            StreamArn = "arn:aws:dynamodb:us-west-2:123456789012:table/users/stream/2023-01-01T00:00:00.000"
        };
        command.Command = JsonSerializer.SerializeToNode(commandConfig);

        // Act
        var result = await strategy.Build<StreamModel>("connector1", command);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(Status.Selecting, result.Status);
        Assert.NotNull(result.Model);
        Assert.Equal("STREAM", result.Model.Operation);
        Assert.Equal("users", result.Model.TableName);
    }

    [Fact]
    public async void BuildModels_WithSequenceNumber_UsesAfterSequenceNumber()
    {
        // Arrange
        var logger = Substitute.For<ILogger<StreamReadStrategy>>();
        var strategy = new StreamReadStrategy(logger);
        
        var command = new CommandRecord { BatchSize = 50 };
        var commandConfig = new CommandConfig
        {
            TableName = "orders",
            StreamArn = "arn:aws:dynamodb:us-west-2:123456789012:table/orders/stream/2023-01-01T00:00:00.000",
            SequenceNumber = "1234567890"
        };
        command.Command = JsonSerializer.SerializeToNode(commandConfig);

        // Act
        var result = await strategy.Build<StreamModel>("connector1", command);

        // Assert
        Assert.Equal("AFTER_SEQUENCE_NUMBER", result.Model.ShardIteratorType);
        Assert.Equal("1234567890", result.Model.SequenceNumber);
    }

    [Fact]
    public async void BuildModels_WithConfiguredIteratorType_UsesConfiguredType()
    {
        // Arrange
        var logger = Substitute.For<ILogger<StreamReadStrategy>>();
        var strategy = new StreamReadStrategy(logger);
        
        var command = new CommandRecord { BatchSize = 100 };
        var commandConfig = new CommandConfig
        {
            TableName = "products",
            StreamArn = "arn:aws:dynamodb:us-west-2:123456789012:table/products/stream/2023-01-01T00:00:00.000",
            ShardIteratorType = "TRIM_HORIZON"
        };
        command.Command = JsonSerializer.SerializeToNode(commandConfig);

        // Act
        var result = await strategy.Build<StreamModel>("connector1", command);

        // Assert
        Assert.Equal("TRIM_HORIZON", result.Model.ShardIteratorType);
    }

    [Fact]
    public async void BuildModels_WithTimestamp_UsesAtTimestamp()
    {
        // Arrange
        var logger = Substitute.For<ILogger<StreamReadStrategy>>();
        var strategy = new StreamReadStrategy(logger);
        
        var command = new CommandRecord { BatchSize = 75 };
        var commandConfig = new CommandConfig
        {
            TableName = "logs",
            StreamArn = "arn:aws:dynamodb:us-west-2:123456789012:table/logs/stream/2023-01-01T00:00:00.000",
            Timestamp = 1609459200000 // A non-zero timestamp
        };
        command.Command = JsonSerializer.SerializeToNode(commandConfig);

        // Act
        var result = await strategy.Build<StreamModel>("connector1", command);

        // Assert
        Assert.Equal("AT_TIMESTAMP", result.Model.ShardIteratorType);
    }

    [Fact]
    public async void BuildModels_WithNoParams_DefaultsToLatest()
    {
        // Arrange
        var logger = Substitute.For<ILogger<StreamReadStrategy>>();
        var strategy = new StreamReadStrategy(logger);
        
        var command = new CommandRecord { BatchSize = 100 };
        var commandConfig = new CommandConfig
        {
            TableName = "events",
            StreamArn = "arn:aws:dynamodb:us-west-2:123456789012:table/events/stream/2023-01-01T00:00:00.000",
            Timestamp = 0 // No timestamp
        };
        command.Command = JsonSerializer.SerializeToNode(commandConfig);

        // Act
        var result = await strategy.Build<StreamModel>("connector1", command);

        // Assert
        Assert.Equal("LATEST", result.Model.ShardIteratorType);
    }

    [Fact]
    public async void BuildModels_SetsBatchSize()
    {
        // Arrange
        var logger = Substitute.For<ILogger<StreamReadStrategy>>();
        var strategy = new StreamReadStrategy(logger);
        
        var command = new CommandRecord { BatchSize = 123 };
        var commandConfig = new CommandConfig
        {
            TableName = "test",
            StreamArn = "arn:aws:dynamodb:us-west-2:123456789012:table/test/stream/2023-01-01T00:00:00.000"
        };
        command.Command = JsonSerializer.SerializeToNode(commandConfig);

        // Act
        var result = await strategy.Build<StreamModel>("connector1", command);

        // Assert
        Assert.Equal(123, result.Model.Request.Limit);
    }

    [Fact]
    public async Task BuildModels_WithConnectRecord_ThrowsNotImplementedException()
    {
        // Arrange
        var logger = Substitute.For<ILogger<StreamReadStrategy>>();
        var strategy = new StreamReadStrategy(logger);
        var record = new ConnectRecord("topic", 0, 0);

        // Act & Assert
        await Assert.ThrowsAsync<NotImplementedException>(async () => 
            await strategy.Build<StreamModel>("connector1", record));
    }

    [Fact]
    public async void BuildModels_SequenceNumberPriorityOverIteratorType()
    {
        // Arrange
        var logger = Substitute.For<ILogger<StreamReadStrategy>>();
        var strategy = new StreamReadStrategy(logger);
        
        var command = new CommandRecord { BatchSize = 100 };
        var commandConfig = new CommandConfig
        {
            TableName = "test",
            StreamArn = "arn:aws:dynamodb:us-west-2:123456789012:table/test/stream/2023-01-01T00:00:00.000",
            SequenceNumber = "abc123",
            ShardIteratorType = "TRIM_HORIZON", // This should be ignored
            Timestamp = 1609459200000 // This should be ignored too
        };
        command.Command = JsonSerializer.SerializeToNode(commandConfig);

        // Act
        var result = await strategy.Build<StreamModel>("connector1", command);

        // Assert
        // Sequence number should take priority
        Assert.Equal("AFTER_SEQUENCE_NUMBER", result.Model.ShardIteratorType);
        Assert.Equal("abc123", result.Model.SequenceNumber);
    }
}
