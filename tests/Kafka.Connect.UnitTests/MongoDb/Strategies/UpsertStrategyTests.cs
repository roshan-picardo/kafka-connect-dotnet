using System;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.MongoDb.Models;
using Kafka.Connect.MongoDb.Strategies;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.MongoDb.Strategies;

public class UpsertStrategyTests
{
    [Fact]
    public async Task Build_WithConnectRecord_ReturnsUpsertModel()
    {
        var configProvider = Substitute.For<IConfigurationProvider>();
        configProvider.GetPluginConfig<PluginConfig>("c1").Returns(new PluginConfig { Filter = "{ id: #id# }" });
        var strategy = new UpsertStrategy(Substitute.For<ILogger<UpsertStrategy>>(), configProvider);

        var record = new ConnectRecord("topic", 0, 0)
        {
            Serialized = new ConnectMessage<byte[]> { Key = [1] },
            Deserialized = new ConnectMessage<JsonNode> { Value = JsonNode.Parse("{\"id\":1,\"name\":\"Jane\"}") }
        };

        var result = await strategy.Build<MongoDB.Driver.UpdateOneModel<MongoDB.Bson.BsonDocument>>("c1", record);

        Assert.Equal(Status.Updating, result.Status);
        Assert.True(result.Model.IsUpsert);
    }

    [Fact]
    public async Task Build_WithCommandRecord_ThrowsNotImplementedException()
    {
        var strategy = new UpsertStrategy(Substitute.For<ILogger<UpsertStrategy>>(), Substitute.For<IConfigurationProvider>());
        await Assert.ThrowsAsync<NotImplementedException>(() => strategy.Build<MongoDB.Driver.UpdateOneModel<MongoDB.Bson.BsonDocument>>("c1", new CommandRecord()));
    }
}
