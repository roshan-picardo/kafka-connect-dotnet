using System;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.MySql.Models;
using Kafka.Connect.MySql.Strategies;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.MySql.Strategies;

public class InsertStrategyTests
{
    [Fact]
    public async Task Build_WithConnectRecord_ReturnsInsertSql()
    {
        var configurationProvider = Substitute.For<IConfigurationProvider>();
        configurationProvider.GetPluginConfig<PluginConfig>("c1").Returns(new PluginConfig
        {
            Schema = "dbo",
            Table = "users"
        });

        var strategy = new InsertStrategy(Substitute.For<ILogger<InsertStrategy>>(), configurationProvider);
        var record = new ConnectRecord("topic", 0, 0)
        {
            Serialized = new ConnectMessage<byte[]> { Key = [1] },
            Deserialized = new ConnectMessage<JsonNode> { Value = JsonNode.Parse("{\"id\":1,\"name\":\"Jane\"}") }
        };

        var result = await strategy.Build<string>("c1", record);

        Assert.Equal(Status.Inserting, result.Status);
        Assert.Contains("INSERT INTO `dbo`.`users`", result.Model);
        Assert.Contains("`id`", result.Model);
        Assert.Contains("`name`", result.Model);
    }

    [Fact]
    public async Task Build_WithCommandRecord_ThrowsNotImplementedException()
    {
        var strategy = new InsertStrategy(Substitute.For<ILogger<InsertStrategy>>(), Substitute.For<IConfigurationProvider>());

        await Assert.ThrowsAsync<NotImplementedException>(() => strategy.Build<string>("c1", new CommandRecord()));
    }
}
