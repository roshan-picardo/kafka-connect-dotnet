using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.MongoDb.Models;
using Kafka.Connect.MongoDb.Strategies;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.MongoDb.Strategies;

public class ReadStrategyTests
{
    [Fact]
    public async Task Build_ForSourceCommand_BuildsFindModel()
    {
        var strategy = new ReadStrategy(Substitute.For<ILogger<ReadStrategy>>());
        var config = new CommandConfig
        {
            Filters = new Dictionary<string, object> { ["id"] = 10, ["seq"] = 5 }
        };
        var command = new CommandRecord { BatchSize = 20, Command = JsonSerializer.SerializeToNode(config) };

        var result = await strategy.Build<FindModel<MongoDB.Bson.BsonDocument>>("c1", command);

        Assert.Equal(Status.Selecting, result.Status);
        Assert.Equal("CHANGE", result.Model.Operation);
        Assert.NotNull(result.Model.Options);
        Assert.Equal(20, result.Model.Options.BatchSize);
    }

    [Fact]
    public async Task Build_ForSourceWithoutFilters_UsesEmptyFilter()
    {
        var strategy = new ReadStrategy(Substitute.For<ILogger<ReadStrategy>>());
        var command = new CommandRecord
        {
            BatchSize = 10,
            Command = JsonSerializer.SerializeToNode(new CommandConfig { Filters = null })
        };

        var result = await strategy.Build<FindModel<MongoDB.Bson.BsonDocument>>("c1", command);

        Assert.NotNull(result.Model.Filter);
        Assert.Null(result.Model.Options.Sort);
    }

    [Fact]
    public async Task Build_ForChangelog_DoesNotBuildModel()
    {
        var strategy = new ReadStrategy(Substitute.For<ILogger<ReadStrategy>>());
        var command = new CommandRecord
        {
            BatchSize = 10,
            Changelog = JsonNode.Parse("{}"),
            Command = JsonSerializer.SerializeToNode(new CommandConfig())
        };

        var result = await strategy.Build<FindModel<MongoDB.Bson.BsonDocument>>("c1", command);

        Assert.Equal(Status.Selecting, result.Status);
        Assert.Null(result.Model);
    }
}
