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

public class DeleteStrategyTests
{
    [Fact]
    public async Task BuildModels_WithConnectRecord_ReturnsDeleteSql()
    {
        var configProvider = Substitute.For<IConfigurationProvider>();
        configProvider.GetPluginConfig<PluginConfig>("c1").Returns(new PluginConfig
        {
            Schema = "DB2INST1",
            Table = "users",
            Filter = "id = '#id#'"
        });
        var sut = new DeleteStrategy(Substitute.For<ILogger<DeleteStrategy>>(), configProvider);
        var record = new ConnectRecord("t", 0, 0)
        {
            Serialized = new ConnectMessage<byte[]> { Key = [1] },
            Deserialized = new ConnectMessage<JsonNode>
            {
                Value = JsonNode.Parse("{\"id\":1}")
            }
        };

        var result = await sut.Build<string>("c1", record);

        Assert.Equal(Status.Deleting, result.Status);
        Assert.Contains("DELETE FROM DB2INST1.users", result.Model);
        Assert.Contains("WHERE id = '1'", result.Model);
    }

    [Fact]
    public async Task BuildModels_WithCommandRecord_ThrowsNotImplementedException()
    {
        var sut = new DeleteStrategy(
            Substitute.For<ILogger<DeleteStrategy>>(),
            Substitute.For<IConfigurationProvider>());

        await Assert.ThrowsAsync<NotImplementedException>(() =>
            sut.Build<string>("c1", new CommandRecord()));
    }
}
