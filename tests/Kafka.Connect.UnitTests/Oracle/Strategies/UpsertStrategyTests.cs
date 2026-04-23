using System;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Oracle.Models;
using Kafka.Connect.Oracle.Strategies;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Oracle.Strategies;

public class UpsertStrategyTests
{
    [Fact]
    public async Task Build_WithConnectRecord_ReturnsUpsertSql()
    {
        var configurationProvider = Substitute.For<IConfigurationProvider>();
        configurationProvider.GetPluginConfig<PluginConfig>("c1").Returns(new PluginConfig
        {
            Schema = "dbo",
            Table = "users",
            Lookup = "id = #id#"
        });

        var strategy = new UpsertStrategy(Substitute.For<ILogger<UpsertStrategy>>(), configurationProvider);
        var record = new ConnectRecord("topic", 0, 0)
        {
            Serialized = new ConnectMessage<byte[]> { Key = [1] },
            Deserialized = new ConnectMessage<JsonNode> { Value = JsonNode.Parse("{\"id\":1,\"name\":\"Jane\"}") }
        };

        var result = await strategy.Build<string>("c1", record);

        Assert.Equal(Status.Updating, result.Status);
        Assert.Contains("MERGE INTO dbo.users", result.Model);
        Assert.Contains("USING DUAL", result.Model);
        Assert.Contains("WHEN NOT  MATCHED THEN", result.Model);
    }

    [Fact]
    public async Task Build_WithCommandRecord_ThrowsNotImplementedException()
    {
        var strategy = new UpsertStrategy(Substitute.For<ILogger<UpsertStrategy>>(), Substitute.For<IConfigurationProvider>());

        await Assert.ThrowsAsync<NotImplementedException>(() => strategy.Build<string>("c1", new CommandRecord()));
    }
}
