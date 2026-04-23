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

public class StreamsReadStrategyTests
{
    [Fact]
    public async Task Build_WithResumeToken_UsesResumeAfter()
    {
        var strategy = new StreamsReadStrategy(Substitute.For<ILogger<StreamsReadStrategy>>());
        var command = new CommandRecord
        {
            BatchSize = 5,
            Command = JsonSerializer.SerializeToNode(new CommandConfig
            {
                Filters = new Dictionary<string, object> { ["_resumeToken"] = "{ \"_data\": \"abc\" }" }
            })
        };

        var result = await strategy.Build<WatchModel>("c1", command);

        Assert.Equal("STREAM", result.Model.Operation);
        Assert.NotNull(result.Model.Options.ResumeAfter);
        Assert.Equal(5, result.Model.Options.BatchSize);
    }

    [Fact]
    public async Task Build_WithTimestamp_UsesStartAtOperationTime()
    {
        var strategy = new StreamsReadStrategy(Substitute.For<ILogger<StreamsReadStrategy>>());
        var command = new CommandRecord
        {
            BatchSize = 5,
            Command = JsonSerializer.SerializeToNode(new CommandConfig { Timestamp = 1234 })
        };

        var result = await strategy.Build<WatchModel>("c1", command);

        Assert.NotNull(result.Model.Options.StartAtOperationTime);
        Assert.Null(result.Model.Options.ResumeAfter);
    }

    [Fact]
    public async Task Build_ForChangelog_DoesNotBuildModel()
    {
        var strategy = new StreamsReadStrategy(Substitute.For<ILogger<StreamsReadStrategy>>());
        var command = new CommandRecord
        {
            BatchSize = 5,
            Changelog = JsonNode.Parse("{}"),
            Command = JsonSerializer.SerializeToNode(new CommandConfig())
        };

        var result = await strategy.Build<WatchModel>("c1", command);

        Assert.Equal(Status.Selecting, result.Status);
        Assert.Null(result.Model);
    }
}
