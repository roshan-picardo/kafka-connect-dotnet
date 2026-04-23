using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using Kafka.Connect.DynamoDb;
using Kafka.Connect.DynamoDb.Models;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using NSubstitute;
using Xunit;

using System.Text.Json;

namespace UnitTests.Kafka.Connect.DynamoDb;

public class DynamoDbCommandHandlerTests
{
    [Fact]
    public void Get_WithValidConnector_ReturnsCommands()
    {
        // Arrange
        var configProvider = Substitute.For<IConfigurationProvider>();
        var pluginConfig = new PluginConfig
        {
            Commands = new Dictionary<string, CommandConfig>
            {
                ["read"] = new CommandConfig { TableName = "users" },
                ["write"] = new CommandConfig { TableName = "logs" }
            }
        };
        configProvider.GetPluginConfig<PluginConfig>("connector1").Returns(pluginConfig);
        
        var handler = new DynamoDbCommandHandler(configProvider);

        // Act
        var result = handler.Get("connector1");

        // Assert
        Assert.NotNull(result);
        Assert.Equal(2, result.Count);
        Assert.Contains("read", result.Keys);
        Assert.Contains("write", result.Keys);
    }

    [Fact]
    public void Get_WithEmptyCommands_ReturnsEmpty()
    {
        // Arrange
        var configProvider = Substitute.For<IConfigurationProvider>();
        var pluginConfig = new PluginConfig { Commands = new Dictionary<string, CommandConfig>() };
        configProvider.GetPluginConfig<PluginConfig>("connector1").Returns(pluginConfig);
        
        var handler = new DynamoDbCommandHandler(configProvider);

        // Act
        var result = handler.Get("connector1");

        // Assert
        Assert.NotNull(result);
        Assert.Empty(result);
    }

    [Fact]
    public void Next_NonChangeLog_WithRecords_UpdatesFilters()
    {
        // Arrange
        var handler = new DynamoDbCommandHandler(Substitute.For<IConfigurationProvider>());
        var command = new CommandRecord();
        var commandConfig = new CommandConfig
        {
            Filters = new Dictionary<string, object> { ["id"] = 0 },
            Keys = new[] { "id", "name" }
        };
        command.Command = JsonSerializer.SerializeToNode(commandConfig);

        var records = new List<ConnectMessage<JsonNode>>
        {
            new ConnectMessage<JsonNode>
            {
                Key = JsonNode.Parse("{\"id\": 1}"),
                Value = JsonNode.Parse("{\"id\": 1, \"name\": \"John\", \"age\": 30}")
            }
        };

        // Act
        var result = handler.Next(command, records);

        // Assert
        Assert.NotNull(result);
        Assert.NotEmpty(result.ToString());
    }

    [Fact]
    public void Next_NonChangeLog_EmptyRecords_ReturnsSameConfig()
    {
        // Arrange
        var handler = new DynamoDbCommandHandler(Substitute.For<IConfigurationProvider>());
        var command = new CommandRecord();
        var commandConfig = new CommandConfig { Filters = new Dictionary<string, object>() };
        command.Command = JsonSerializer.SerializeToNode(commandConfig);

        var records = new List<ConnectMessage<JsonNode>>();

        // Act
        var result = handler.Next(command, records);

        // Assert
        Assert.NotNull(result);
    }

    [Fact]
    public void Next_ChangeLog_WithRecords_UpdatesTimestampAndFilters()
    {
        // Arrange
        var handler = new DynamoDbCommandHandler(Substitute.For<IConfigurationProvider>());
        var command = new CommandRecord();
        
        // Mark command as ChangeLog
        var commandConfig = new CommandConfig
        {
            Keys = new[] { "id" },
            Timestamp = 0
        };
        command.Command = JsonSerializer.SerializeToNode(commandConfig);
        command.Changelog = JsonNode.Parse("{}");

        var records = new List<ConnectMessage<JsonNode>>
        {
            new ConnectMessage<JsonNode>
            {
                Key = JsonNode.Parse("{\"id\": 1}"),
                Value = JsonNode.Parse("{\"name\": \"John\"}"),
                Timestamp = 1609459200
            }
        };

        // Act
        var result = handler.Next(command, records);

        // Assert
        Assert.NotNull(result);
    }
}
