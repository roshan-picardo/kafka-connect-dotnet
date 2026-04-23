using System.Collections.Generic;
using System.Text.Json.Nodes;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Postgres.Strategies;
using Xunit;

namespace UnitTests.Kafka.Connect.Postgres.Strategies;

public class ChangelogStrategySelectorTests
{
    [Theory]
    [InlineData("INSERT")]
    [InlineData("UPDATE")]
    [InlineData("DELETE")]
    [InlineData("IMPORT")]
    [InlineData("CHANGE")]
    public void GetStrategy_ReturnsExpectedStrategy(string operation)
    {
        var selector = new ChangelogStrategySelector(new List<IStrategy>
        {
            new InsertStrategy(null, null),
            new UpdateStrategy(null, null),
            new DeleteStrategy(null, null),
            new UpsertStrategy(null, null)
        });

        var result = selector.GetStrategy(new ConnectRecord("topic", 0, 0)
        {
            Raw = new ConnectMessage<JsonNode> { Value = JsonNode.Parse($"{{\"operation\":\"{operation}\"}}") }
        }, new Dictionary<string, string>());

        Assert.NotNull(result);
    }

    [Fact]
    public void GetStrategy_WhenUnknownOperation_ReturnsNullWhenSkipStrategyAbsent()
    {
        var selector = new ChangelogStrategySelector([]);

        var result = selector.GetStrategy(new ConnectRecord("topic", 0, 0)
        {
            Raw = new ConnectMessage<JsonNode> { Value = JsonNode.Parse("{\"operation\":\"unknown\"}") }
        }, new Dictionary<string, string>());

        Assert.Null(result);
    }
}