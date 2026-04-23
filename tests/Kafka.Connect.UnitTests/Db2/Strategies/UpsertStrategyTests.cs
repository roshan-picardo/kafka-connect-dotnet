using System;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Db2.Models;
using Kafka.Connect.Db2.Strategies;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Db2.Strategies;

public class UpsertStrategyTests
{
    [Fact]
    public async Task BuildModels_WithConnectRecord_ReturnsUpsertSql()
    {
        var configProvider = Substitute.For<IConfigurationProvider>();
        configProvider.GetPluginConfig<PluginConfig>("c1").Returns(new PluginConfig
        {
            Schema = "DB2INST1",
            Table = "users",
            Lookup = "id = '#id#'"
        });
        var sut = new UpsertStrategy(Substitute.For<ILogger<UpsertStrategy>>(), configProvider);
        var record = new ConnectRecord("t", 0, 0)
        {
            Serialized = new ConnectMessage<byte[]> { Key = [1] },
            Deserialized = new ConnectMessage<JsonNode>
            {
                Value = JsonNode.Parse("{\"id\":1,\"name\":\"Alice\"}")
            }
        };

        var result = await sut.Build<string>("c1", record);

        Assert.Equal(Status.Updating, result.Status);
        Assert.Contains("MERGE INTO DB2INST1.users AS target", result.Model);
        Assert.Contains("WHEN MATCHED THEN", result.Model);
        Assert.Contains("WHEN NOT MATCHED THEN", result.Model);
    }

    [Fact]
    public async Task BuildModels_WithCommandRecord_ThrowsNotImplementedException()
    {
        var sut = new UpsertStrategy(
            Substitute.For<ILogger<UpsertStrategy>>(),
            Substitute.For<IConfigurationProvider>());

        await Assert.ThrowsAsync<NotImplementedException>(() =>
            sut.Build<string>("c1", new CommandRecord()));
    }
}
