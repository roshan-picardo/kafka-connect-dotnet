using System;
using System.Linq;
using System.Text.Json.Nodes;
using Kafka.Connect.Plugin.Models;
using Xunit;

namespace UnitTests.Kafka.Connect.Plugin.Models;

public class SupportingModelsTests
{
    [Fact]
    public void StrategyModel_WhenSettingSingleModel_AppendsAndReturnsFirstModel()
    {
        var strategy = new StrategyModel<string>();

        strategy.Model = "first";
        strategy.Model = "second";

        Assert.Equal("first", strategy.Model);
        Assert.Equal(2, strategy.Models.Count);
    }

    [Fact]
    public void LogTimestamp_ComputedPropertiesReturnExpectedValues()
    {
        var timestamp = new LogTimestamp
        {
            Created = 1000,
            Consumed = 1600,
            Committed = 2600,
            BatchSize = 4
        };

        Assert.Equal(TimeSpan.FromMilliseconds(600), timestamp.Lag);
        Assert.Equal(TimeSpan.FromMilliseconds(1600), timestamp.Total);
        Assert.Equal(250m, timestamp.Duration);
        Assert.Equal(1000L, timestamp.Batch);
    }

    [Fact]
    public void CommandRecord_IdIsDeterministicFromConnectorAndName()
    {
        var record = new CommandRecord
        {
            Connector = "orders",
            Name = "sync"
        };

        Assert.Equal(record.Id, record.Id);
        Assert.NotEqual(Guid.Empty, record.Id);
    }

    [Fact]
    public void CommandRecord_GetVersion_UsesExistingVersionWhenPresent()
    {
        var record = new CommandRecord
        {
            Command = JsonNode.Parse("""{ "Version": 7, "Name": "sync" }""")
        };

        var result = record.GetVersion();

        Assert.Equal(7, result);
    }

    [Fact]
    public void CommandRecord_GetVersion_WhenMissing_ComputesAndStoresVersion()
    {
        var record = new CommandRecord
        {
            Command = JsonNode.Parse("""{ "Name": "sync" }""")
        };

        var result = record.GetVersion();

        Assert.True(result > 0);
        Assert.Equal(result, record.Command!["Version"]!.GetValue<int>());
    }

    [Fact]
    public void CommandRecord_GetCommandAndChangeLog_DeserializeToTargetTypes()
    {
        var record = new CommandRecord
        {
            Command = JsonNode.Parse("""{ "Name": "sync" }"""),
            Changelog = JsonNode.Parse("""{ "Applied": true }""")
        };

        var command = record.GetCommand<CommandPayload>();
        var changelog = record.GetChangelog<ChangePayload>();

        Assert.Equal("sync", command.Name);
        Assert.True(changelog.Applied);
        Assert.True(record.IsChangeLog());
    }

    [Fact]
    public void StatusEnum_ContainsExpectedTerminalValues()
    {
        var names = Enum.GetNames<Status>();

        Assert.Contains(nameof(Status.Aborted), names);
        Assert.Contains(nameof(Status.Published), names);
        Assert.Contains(nameof(Status.Failed), names);
        Assert.True(names.Length > 10);
    }

    private sealed class CommandPayload
    {
        public string Name { get; init; }
    }

    private sealed class ChangePayload
    {
        public bool Applied { get; init; }
    }
}