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

public class UpdateStrategyTests
{
    [Fact]
    public async Task Build_WithConnectRecord_ReturnsUpdateSql()
    {
        var logger = Substitute.For<ILogger<UpdateStrategy>>();
        var configProvider = Substitute.For<IConfigurationProvider>();
        configProvider.GetPluginConfig<PluginConfig>("c1").Returns(new PluginConfig { Schema = "s", Table = "t", Filter = "id=#id#" });

        var strategy = new UpdateStrategy(logger, configProvider);
        var record = new ConnectRecord("topic", 0, 0)
        {
            Serialized = new ConnectMessage<byte[]> { Key = [1] },
            Deserialized = new ConnectMessage<JsonNode>
            {
                Value = JsonNode.Parse("{\"id\":1,\"name\":\"Jane\"}")
            }
        };

        var result = await strategy.Build<string>("c1", record);

        Assert.Equal(Status.Updating, result.Status);
        Assert.Contains("UPDATE `s`.`t`", result.Model);
        Assert.Contains("WHERE id=1", result.Model);
    }

    [Fact]
    public async Task Build_WithCommandRecord_ThrowsNotImplemented()
    {
        var strategy = new UpdateStrategy(Substitute.For<ILogger<UpdateStrategy>>(), Substitute.For<IConfigurationProvider>());

        await Assert.ThrowsAsync<NotImplementedException>(() => strategy.Build<string>("c1", new CommandRecord()));
    }
}
