using System.Collections.Generic;
using System.Text.Json;
using Kafka.Connect.DynamoDb.Models;
using Kafka.Connect.Plugin.Models;
using Xunit;

namespace UnitTests.Kafka.Connect.DynamoDb.Models;

public class ConfigurationModelsTests
{
    [Fact]
    public void PluginConfig_CreateWithProperties_AllPropertiesSet()
    {
        // Arrange & Act
        var config = new PluginConfig
        {
            Region = "us-west-2",
            AccessKeyId = "AKIAIOSFODNN7EXAMPLE",
            SecretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            ServiceUrl = "http://localhost:8000",
            TableName = "my-table",
            IsWriteOrdered = false,
            Filter = "id={id}",
            Commands = new Dictionary<string, CommandConfig>
            {
                ["read"] = new CommandConfig { TableName = "test-table" }
            }
        };

        // Assert
        Assert.Equal("us-west-2", config.Region);
        Assert.Equal("AKIAIOSFODNN7EXAMPLE", config.AccessKeyId);
        Assert.Equal("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", config.SecretAccessKey);
        Assert.Equal("http://localhost:8000", config.ServiceUrl);
        Assert.Equal("my-table", config.TableName);
        Assert.False(config.IsWriteOrdered);
        Assert.Equal("id={id}", config.Filter);
        Assert.Single(config.Commands);
    }

    [Fact]
    public void PluginConfig_DefaultIsWriteOrdered_True()
    {
        // Arrange & Act
        var config = new PluginConfig();

        // Assert
        Assert.True(config.IsWriteOrdered);
    }

    [Fact]
    public void CommandConfig_InheritsFromCommand_CanSetAllProperties()
    {
        // Arrange & Act
        var commandConfig = new CommandConfig
        {
            Timestamp = 1234567890,
            Filters = new Dictionary<string, object> { ["name"] = "John" },
            TableName = "users",
            TimestampColumn = "updated_at",
            Keys = new[] { "id", "version" },
            IndexName = "user-index",
            UseStreams = true,
            StreamArn = "arn:aws:dynamodb:us-west-2:123456789012:table/MyTable/stream/2015-05-11T21:21:33.291",
            ShardIteratorType = "TRIM_HORIZON",
            SequenceNumber = "12345678"
        };

        // Assert
        Assert.Equal(1234567890, commandConfig.Timestamp);
        Assert.Equal("users", commandConfig.TableName);
        Assert.Equal("updated_at", commandConfig.TimestampColumn);
        Assert.Equal(new[] { "id", "version" }, commandConfig.Keys);
        Assert.Equal("user-index", commandConfig.IndexName);
        Assert.True(commandConfig.UseStreams);
        Assert.Equal("arn:aws:dynamodb:us-west-2:123456789012:table/MyTable/stream/2015-05-11T21:21:33.291", commandConfig.StreamArn);
        Assert.Equal("TRIM_HORIZON", commandConfig.ShardIteratorType);
        Assert.Equal("12345678", commandConfig.SequenceNumber);
    }

    [Fact]
    public void CommandConfig_ToJson_ReturnsSerializedJsonNode()
    {
        // Arrange
        var commandConfig = new CommandConfig
        {
            Timestamp = 1234567890,
            TableName = "users",
            TimestampColumn = "updated_at",
            Keys = new[] { "id" },
            UseStreams = false,
            ShardIteratorType = "LATEST"
        };

        // Act
        var jsonNode = commandConfig.ToJson();

        // Assert
        Assert.NotNull(jsonNode);
        Assert.Equal(1234567890, jsonNode["Timestamp"].GetValue<long>());
        Assert.Equal("users", jsonNode["TableName"].GetValue<string>());
    }

    [Fact]
    public void CommandConfig_FiltersAndTimestampUpdate_WorkCorrectly()
    {
        // Arrange
        var commandConfig = new CommandConfig { Timestamp = 0 };

        // Act
        commandConfig.Filters = new Dictionary<string, object>
        {
            { "created_at", "2023-01-01" },
            { "status", "active" }
        };
        commandConfig.Timestamp = 1609459200;

        // Assert
        Assert.Equal(2, commandConfig.Filters.Count);
        Assert.Equal("2023-01-01", commandConfig.Filters["created_at"]);
        Assert.Equal(1609459200, commandConfig.Timestamp);
    }
}
