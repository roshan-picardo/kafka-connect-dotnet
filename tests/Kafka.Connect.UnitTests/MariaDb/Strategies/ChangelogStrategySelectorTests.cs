using System.Collections.Generic;
using System.Text.Json.Nodes;
using Kafka.Connect.MariaDb.Strategies;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.MariaDb.Strategies;

public class ChangelogStrategySelectorTests
{
    [Theory]
    [InlineData("INSERT", typeof(InsertStrategy))]
    [InlineData("UPDATE", typeof(UpdateStrategy))]
    [InlineData("DELETE", typeof(DeleteStrategy))]
    [InlineData("IMPORT", typeof(UpsertStrategy))]
    [InlineData("CHANGE", typeof(UpsertStrategy))]
    public void GetStrategy_ForOperation_ReturnsExpectedType(string operation, System.Type expected)
    {
        var strategies = new List<IStrategy>
        {
            new InsertStrategy(Substitute.For<ILogger<InsertStrategy>>(), Substitute.For<global::Kafka.Connect.Plugin.Providers.IConfigurationProvider>()),
            new UpdateStrategy(Substitute.For<ILogger<UpdateStrategy>>(), Substitute.For<global::Kafka.Connect.Plugin.Providers.IConfigurationProvider>()),
            new DeleteStrategy(Substitute.For<ILogger<DeleteStrategy>>(), Substitute.For<global::Kafka.Connect.Plugin.Providers.IConfigurationProvider>()),
            new UpsertStrategy(Substitute.For<ILogger<UpsertStrategy>>(), Substitute.For<global::Kafka.Connect.Plugin.Providers.IConfigurationProvider>())
        };

        var selector = new ChangelogStrategySelector(strategies, Substitute.For<ILogger<ChangelogStrategySelector>>());
        var record = new ConnectRecord("t", 0, 0)
        {
            Raw = new ConnectMessage<JsonNode> { Value = JsonNode.Parse($"{{\"operation\":\"{operation}\"}}") }
        };

        var result = selector.GetStrategy(record);

        Assert.NotNull(result);
        Assert.Equal(expected, result.GetType());
    }

    [Fact]
    public void GetStrategy_ForUnknownOperation_ReturnsNullWhenSkipStrategyMissing()
    {
        var selector = new ChangelogStrategySelector([], Substitute.For<ILogger<ChangelogStrategySelector>>());
        var record = new ConnectRecord("t", 0, 0)
        {
            Raw = new ConnectMessage<JsonNode> { Value = JsonNode.Parse("{\"operation\":\"OTHER\"}") }
        };

        var result = selector.GetStrategy(record);

        Assert.Null(result);
    }
}
