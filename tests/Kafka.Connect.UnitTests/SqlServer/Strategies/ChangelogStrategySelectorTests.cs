using System.Collections.Generic;
using System.Text.Json.Nodes;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.SqlServer.Strategies;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.SqlServer.Strategies;

public class ChangelogStrategySelectorTests
{
    private static ChangelogStrategySelector BuildSelector() => new(
        new List<IStrategy>
        {
            new InsertStrategy(null, null),
            new UpdateStrategy(null, null),
            new DeleteStrategy(null, null),
            new UpsertStrategy(null, null)
        },
        Substitute.For<ILogger<ChangelogStrategySelector>>());

    [Theory]
    [InlineData("INSERT")]
    [InlineData("UPDATE")]
    [InlineData("DELETE")]
    public void GetStrategy_ForSinkOperation_ReturnsDmlStrategy(string operation)
    {
        var selector = BuildSelector();

        var result = selector.GetStrategy(new ConnectRecord("t", 0, 0)
        {
            Raw = new ConnectMessage<JsonNode> { Value = JsonNode.Parse($"{{\"operation\":\"{operation}\"}}") }
        });

        Assert.NotNull(result);
    }

    [Theory]
    [InlineData("IMPORT")]
    [InlineData("CHANGE")]
    public void GetStrategy_ForImportOrChange_ReturnsUpsertStrategy(string operation)
    {
        var selector = BuildSelector();

        var result = selector.GetStrategy(new ConnectRecord("t", 0, 0)
        {
            Raw = new ConnectMessage<JsonNode> { Value = JsonNode.Parse($"{{\"operation\":\"{operation}\"}}") }
        });

        Assert.IsType<UpsertStrategy>(result);
    }

    [Fact]
    public void GetStrategy_ForUnknownOperation_ReturnsNullWhenSkipAbsent()
    {
        var selector = new ChangelogStrategySelector(
            [],
            Substitute.For<ILogger<ChangelogStrategySelector>>());

        var result = selector.GetStrategy(new ConnectRecord("t", 0, 0)
        {
            Raw = new ConnectMessage<JsonNode> { Value = JsonNode.Parse("{\"operation\":\"UNKNOWN\"}") }
        });

        Assert.Null(result);
    }
}

