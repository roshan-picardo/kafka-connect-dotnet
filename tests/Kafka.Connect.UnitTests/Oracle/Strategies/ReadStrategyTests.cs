using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Oracle.Models;
using Kafka.Connect.Oracle.Strategies;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Oracle.Strategies;

public class ReadStrategyTests
{
    [Fact]
    public async Task Build_ForSourceCommandWithFilters_BuildsSelect()
    {
        var strategy = new ReadStrategy(Substitute.For<ILogger<ReadStrategy>>());
        var command = new CommandRecord
        {
            BatchSize = 20,
            Command = JsonSerializer.SerializeToNode(new CommandConfig
            {
                Schema = "dbo",
                Table = "users",
                Filters = new Dictionary<string, object> { ["id"] = 10, ["seq"] = 5 }
            })
        };

        var result = await strategy.Build<string>("c1", command);

        Assert.Equal(Status.Selecting, result.Status);
        Assert.Contains("FROM dbo.users", result.Model);
        Assert.Contains("ORDER BY \"id\",\"seq\"", result.Model);
        Assert.Contains("FETCH FIRST 20 ROWS ONLY", result.Model);
    }

    [Fact]
    public async Task Build_ForInitialSnapshot_BuildsCountQuery()
    {
        var strategy = new ReadStrategy(Substitute.For<ILogger<ReadStrategy>>());
        var command = new CommandRecord
        {
            Changelog = JsonNode.Parse("{}"),
            BatchSize = 10,
            Command = JsonSerializer.SerializeToNode(new CommandConfig
            {
                Schema = "dbo",
                Table = "users",
                Snapshot = new SnapshotConfig { Enabled = true, Total = 0 }
            })
        };

        var result = await strategy.Build<string>("c1", command);

        Assert.Contains("COUNT(*) AS \"_total\"", result.Model);
    }

    [Fact]
    public async Task Build_ForSnapshotWithoutKey_BuildsWindowedImportQuery()
    {
        var strategy = new ReadStrategy(Substitute.For<ILogger<ReadStrategy>>());
        var command = new CommandRecord
        {
            Changelog = JsonNode.Parse("{}"),
            BatchSize = 5,
            Command = JsonSerializer.SerializeToNode(new CommandConfig
            {
                Schema = "dbo",
                Table = "users",
                Snapshot = new SnapshotConfig { Enabled = true, Total = 100, Id = 7, Key = null }
            })
        };

        var result = await strategy.Build<string>("c1", command);

        Assert.Contains("'IMPORT' AS \"operation\"", result.Model);
        Assert.Contains("ROWNUM AS \"id\"", result.Model);
    }

    [Fact]
    public async Task Build_WithoutSnapshot_BuildsSourceQuery()
    {
        var strategy = new ReadStrategy(Substitute.For<ILogger<ReadStrategy>>());
        var command = new CommandRecord
        {
            BatchSize = 100,
            Command = JsonSerializer.SerializeToNode(new CommandConfig
            {
                Schema = "dbo",
                Table = "users",
                Snapshot = new SnapshotConfig { Enabled = false }
            })
        };

        var result = await strategy.Build<string>("c1", command);

        Assert.Equal(Status.Selecting, result.Status);
        Assert.Contains("FROM dbo.users", result.Model);
    }

    [Fact]
    public async Task Build_WithConnectRecord_ThrowsNotImplementedException()
    {
        var strategy = new ReadStrategy(Substitute.For<ILogger<ReadStrategy>>());

        await Assert.ThrowsAsync<System.NotImplementedException>(() => 
            strategy.Build<string>("c1", new ConnectRecord("topic", 0, 0)));
    }
}
