using System;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.MongoDb.Strategies;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.MongoDb.Strategies;

public class InsertStrategyTests
{
    [Fact]
    public async Task Build_WithConnectRecord_ReturnsInsertModel()
    {
        var strategy = new InsertStrategy(Substitute.For<ILogger<InsertStrategy>>());
        var record = new ConnectRecord("topic", 0, 0)
        {
            Serialized = new ConnectMessage<byte[]> { Key = [1] },
            Deserialized = new ConnectMessage<JsonNode> { Value = JsonNode.Parse("{\"id\":1,\"name\":\"Jane\"}") }
        };

        var result = await strategy.Build<MongoDB.Driver.InsertOneModel<MongoDB.Bson.BsonDocument>>("c1", record);

        Assert.Equal(Status.Inserting, result.Status);
        Assert.NotNull(result.Model);
    }

    [Fact]
    public async Task Build_WithCommandRecord_ThrowsNotImplementedException()
    {
        var strategy = new InsertStrategy(Substitute.For<ILogger<InsertStrategy>>());
        await Assert.ThrowsAsync<NotImplementedException>(() => strategy.Build<MongoDB.Driver.InsertOneModel<MongoDB.Bson.BsonDocument>>("c1", new CommandRecord()));
    }
}
