using System;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.MariaDb.Models;
using Kafka.Connect.MariaDb.Strategies;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.MariaDb.Strategies;

public class DeleteStrategyTests
{
    [Fact]
    public async Task Build_WithConnectRecord_ReturnsDeleteSql()
    {
        var logger = Substitute.For<ILogger<DeleteStrategy>>();
        var configProvider = Substitute.For<IConfigurationProvider>();
        configProvider.GetPluginConfig<PluginConfig>("c1").Returns(new PluginConfig { Schema = "s", Table = "t", Filter = "id=#id#" });

        var strategy = new DeleteStrategy(logger, configProvider);
        var record = new ConnectRecord("topic", 0, 0)
        {
            Serialized = new ConnectMessage<byte[]> { Key = [1] },
            Deserialized = new ConnectMessage<JsonNode>
            {
                Value = JsonNode.Parse("{\"id\":1}")
            }
        };

        var result = await strategy.Build<string>("c1", record);

        Assert.Equal(Status.Deleting, result.Status);
        Assert.Contains("DELETE FROM `s`.`t`", result.Model);
        Assert.Contains("WHERE id=1", result.Model);
    }

    [Fact]
    public async Task Build_WithCommandRecord_ThrowsNotImplemented()
    {
        var strategy = new DeleteStrategy(Substitute.For<ILogger<DeleteStrategy>>(), Substitute.For<IConfigurationProvider>());

        await Assert.ThrowsAsync<NotImplementedException>(() => strategy.Build<string>("c1", new CommandRecord()));
    }
}
