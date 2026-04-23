using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.MySql.Models;
using Kafka.Connect.MySql.Strategies;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.MySql.Strategies;

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
        Assert.Contains("ORDER BY `id`,`seq`", result.Model);
        Assert.Contains("LIMIT 20", result.Model);
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

        Assert.Contains("WHERE id BETWEEN 8 AND 12", result.Model);
        Assert.Contains("'IMPORT' AS operation", result.Model);
    }

    [Fact]
    public async Task Build_ForSnapshotWithKey_BuildsKeyRangeQuery()
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
                Snapshot = new SnapshotConfig { Enabled = true, Total = 100, Id = 3, Key = "id" }
            })
        };

        var result = await strategy.Build<string>("c1", command);

        Assert.Contains("WHERE id BETWEEN 4 AND 8", result.Model);
    }

    [Fact]
    public async Task Build_ForChangelogPolling_BuildsAuditSelect()
    {
        var strategy = new ReadStrategy(Substitute.For<ILogger<ReadStrategy>>());
        var command = new CommandRecord
        {
            Changelog = JsonSerializer.SerializeToNode(new ChangelogConfig { Schema = "audit", Table = "log" }),
            BatchSize = 7,
            Command = JsonSerializer.SerializeToNode(new CommandConfig
            {
                Schema = "dbo",
                Table = "users",
                Snapshot = new SnapshotConfig { Enabled = false, Timestamp = 100, Id = 2 }
            })
        };

        var result = await strategy.Build<string>("c1", command);

        Assert.Contains("FROM audit.log", result.Model);
        Assert.Contains("log_schema='dbo'", result.Model);
        Assert.Contains("log_table='users'", result.Model);
        Assert.Contains("log_id > 2", result.Model);
        Assert.Contains("LIMIT 7", result.Model);
    }

    [Fact]
    public async Task Build_WithConnectRecord_ThrowsNotImplementedException()
    {
        var strategy = new ReadStrategy(Substitute.For<ILogger<ReadStrategy>>());

        await Assert.ThrowsAsync<System.NotImplementedException>(() => strategy.Build<string>("c1", new ConnectRecord("t", 0, 0)));
    }
}
