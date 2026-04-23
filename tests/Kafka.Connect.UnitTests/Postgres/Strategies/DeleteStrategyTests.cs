using System;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Postgres.Models;
using Kafka.Connect.Postgres.Strategies;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Postgres.Strategies;

public class DeleteStrategyTests
{
    [Fact]
    public async Task Build_WithConnectRecord_ReturnsDeleteSql()
    {
        var configurationProvider = Substitute.For<IConfigurationProvider>();
        configurationProvider.GetPluginConfig<PluginConfig>("c1").Returns(new PluginConfig
        {
            Schema = "public",
            Table = "users",
            Filter = "id = #id#"
        });

        var strategy = new DeleteStrategy(Substitute.For<ILogger<DeleteStrategy>>(), configurationProvider);
        var record = new ConnectRecord("topic", 0, 0)
        {
            Serialized = new ConnectMessage<byte[]> { Key = [1] },
            Deserialized = new ConnectMessage<JsonNode> { Value = JsonNode.Parse("{\"id\":1}") }
        };

        var result = await strategy.Build<string>("c1", record);

        Assert.Equal(Status.Deleting, result.Status);
        Assert.Contains("DELETE FROM public.users", result.Model);
        Assert.Contains("WHERE id = 1", result.Model);
    }

    [Fact]
    public async Task Build_WithCommandRecord_ThrowsNotImplementedException()
    {
        var strategy = new DeleteStrategy(Substitute.For<ILogger<DeleteStrategy>>(), Substitute.For<IConfigurationProvider>());

        await Assert.ThrowsAsync<NotImplementedException>(() => strategy.Build<string>("c1", new CommandRecord()));
    }
}