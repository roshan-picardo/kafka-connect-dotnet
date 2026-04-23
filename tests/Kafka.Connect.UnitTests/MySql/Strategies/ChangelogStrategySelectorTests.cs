using System.Collections.Generic;
using Kafka.Connect.MySql.Strategies;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.MySql.Strategies;

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
        var configurationProvider = Substitute.For<global::Kafka.Connect.Plugin.Providers.IConfigurationProvider>();
        var insert = new InsertStrategy(Substitute.For<ILogger<InsertStrategy>>(), configurationProvider);
        var update = new UpdateStrategy(Substitute.For<ILogger<UpdateStrategy>>(), configurationProvider);
        var delete = new DeleteStrategy(Substitute.For<ILogger<DeleteStrategy>>(), configurationProvider);
        var upsert = new UpsertStrategy(Substitute.For<ILogger<UpsertStrategy>>(), configurationProvider);

        var selector = new ChangelogStrategySelector(
            new List<IStrategy> { insert, update, delete, upsert },
            Substitute.For<ILogger<ChangelogStrategySelector>>());

        var result = selector.GetStrategy(new ConnectRecord("topic", 0, 0)
        {
            Raw = new ConnectMessage<System.Text.Json.Nodes.JsonNode>
            {
                Value = System.Text.Json.Nodes.JsonNode.Parse($"{{\"operation\":\"{operation}\"}}")
            }
        });

        Assert.NotNull(result);
    }

    [Fact]
    public void GetStrategy_WhenUnknownOperation_ReturnsNullWhenSkipStrategyAbsent()
    {
        var selector = new ChangelogStrategySelector(
            new List<IStrategy>(),
            Substitute.For<ILogger<ChangelogStrategySelector>>());

        var result = selector.GetStrategy(new ConnectRecord("topic", 0, 0)
        {
            Raw = new ConnectMessage<System.Text.Json.Nodes.JsonNode>
            {
                Value = System.Text.Json.Nodes.JsonNode.Parse("{\"operation\":\"OTHER\"}")
            }
        });

        Assert.Null(result);
    }
}
