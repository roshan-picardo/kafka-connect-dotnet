using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kafka.Connect.MongoDb;
using Kafka.Connect.MongoDb.Models;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.MongoDb;

public class MongoCommandHandlerTests
{
    [Fact]
    public void Get_ReturnsConfiguredCommands()
    {
        var configProvider = Substitute.For<IConfigurationProvider>();
        configProvider.GetPluginConfig<PluginConfig>("c1").Returns(new PluginConfig
        {
            Commands = new Dictionary<string, CommandConfig> { ["read"] = new() { Collection = "users" } }
        });

        var sut = new MongoCommandHandler(configProvider);

        var result = sut.Get("c1");

        Assert.Single(result);
    }

    [Fact]
    public void Next_ForSourceWithoutRecords_LeavesFiltersUnchanged()
    {
        var sut = new MongoCommandHandler(Substitute.For<IConfigurationProvider>());
        var config = new CommandConfig { Filters = new Dictionary<string, object> { ["id"] = 1 } };
        var command = new CommandRecord { Command = JsonSerializer.SerializeToNode(config) };

        var next = sut.Next(command, []);

        Assert.Equal(1, next!["Filters"]!["id"]!.GetValue<int>());
    }

    [Fact]
    public void Next_ForSourceRecords_UpdatesFiltersFromAfter()
    {
        var sut = new MongoCommandHandler(Substitute.For<IConfigurationProvider>());
        var config = new CommandConfig { Filters = new Dictionary<string, object> { ["id"] = 0 } };
        var command = new CommandRecord { Command = JsonSerializer.SerializeToNode(config) };
        var records = new List<ConnectMessage<JsonNode>>
        {
            new() { Value = JsonNode.Parse("{\"after\":{\"id\":3}}") },
            new() { Value = JsonNode.Parse("{\"after\":{\"id\":5}}") }
        };

        var next = sut.Next(command, records);

        Assert.Equal(5, next!["Filters"]!["id"]!.GetValue<int>());
    }

    [Fact]
    public void Next_ForSourceChangeStream_StoresResumeTokenAndTimestamp()
    {
        var sut = new MongoCommandHandler(Substitute.For<IConfigurationProvider>());
        var config = new CommandConfig { Filters = new Dictionary<string, object>() };
        var command = new CommandRecord { Command = JsonSerializer.SerializeToNode(config) };
        var records = new List<ConnectMessage<JsonNode>>
        {
            new() {
                Timestamp = 77,
                Value = JsonNode.Parse("{\"_resumeToken\":{\"_data\":\"abc\"},\"after\":{\"id\":1}}")
            }
        };

        var next = sut.Next(command, records);

        Assert.Equal(77, next!["Timestamp"]!.GetValue<long>());
        Assert.NotNull(next["Filters"]!["_resumeToken"]);
    }

    [Fact]
    public void Next_ForChangelog_UpdatesTimestampAndFilters()
    {
        var sut = new MongoCommandHandler(Substitute.For<IConfigurationProvider>());
        var config = new CommandConfig
        {
            Keys = ["id"],
            Filters = new Dictionary<string, object> { ["id"] = 0 }
        };
        var command = new CommandRecord
        {
            Command = JsonSerializer.SerializeToNode(config),
            Changelog = JsonNode.Parse("{}")
        };
        var records = new List<ConnectMessage<JsonNode>>
        {
            new() { Timestamp = 10, Key = JsonNode.Parse("{\"id\":1}") },
            new() { Timestamp = 20, Key = JsonNode.Parse("{\"id\":2}") }
        };

        var next = sut.Next(command, records);

        Assert.Equal(20, next!["Timestamp"]!.GetValue<long>());
        Assert.Equal(2, next["Filters"]!["id"]!.GetValue<int>());
    }
}
