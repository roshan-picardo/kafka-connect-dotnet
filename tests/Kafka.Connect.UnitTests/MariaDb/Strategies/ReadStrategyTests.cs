using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.MariaDb.Models;
using Kafka.Connect.MariaDb.Strategies;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.MariaDb.Strategies;

public class ReadStrategyTests
{
    [Fact]
    public async Task Build_ForSourceCommand_BuildsSelectQuery()
    {
        var strategy = new ReadStrategy(Substitute.For<ILogger<ReadStrategy>>());
        var config = new CommandConfig
        {
            Schema = "s",
            Table = "users",
            Filters = new() { ["id"] = 10 }
        };
        var command = new CommandRecord { BatchSize = 20, Command = JsonSerializer.SerializeToNode(config) };

        var result = await strategy.Build<string>("c1", command);

        Assert.Equal(Status.Selecting, result.Status);
        Assert.Contains("FROM s.users", result.Model);
        Assert.Contains("LIMIT 20", result.Model);
    }

    [Fact]
    public async Task Build_ForInitialSnapshot_BuildsCountQuery()
    {
        var strategy = new ReadStrategy(Substitute.For<ILogger<ReadStrategy>>());
        var config = new CommandConfig
        {
            Schema = "s",
            Table = "users",
            Snapshot = new SnapshotConfig { Enabled = true, Total = 0 }
        };
        var command = new CommandRecord
        {
            BatchSize = 10,
            Changelog = JsonNode.Parse("{}"),
            Command = JsonSerializer.SerializeToNode(config)
        };

        var result = await strategy.Build<string>("c1", command);

        Assert.Contains("COUNT(*) AS \"_total\"", result.Model);
    }

    [Fact]
    public async Task Build_ForSnapshotBatch_BuildsImportQuery()
    {
        var strategy = new ReadStrategy(Substitute.For<ILogger<ReadStrategy>>());
        var config = new CommandConfig
        {
            Schema = "s",
            Table = "users",
            Snapshot = new SnapshotConfig { Enabled = true, Total = 100, Id = 5, Key = "id" }
        };
        var command = new CommandRecord
        {
            BatchSize = 10,
            Changelog = JsonNode.Parse("{}"),
            Command = JsonSerializer.SerializeToNode(config)
        };

        var result = await strategy.Build<string>("c1", command);

        Assert.Contains("'IMPORT' AS operation", result.Model);
        Assert.Contains("BETWEEN 6 AND 15", result.Model);
    }

    [Fact]
    public async Task Build_ForChangelog_BuildsAuditQuery()
    {
        var strategy = new ReadStrategy(Substitute.For<ILogger<ReadStrategy>>());
        var config = new CommandConfig
        {
            Schema = "s",
            Table = "users",
            Snapshot = new SnapshotConfig { Enabled = false, Timestamp = 9, Id = 2 }
        };
        var command = new CommandRecord
        {
            BatchSize = 10,
            Changelog = JsonSerializer.SerializeToNode(new ChangelogConfig { Schema = "log", Table = "audit" }),
            Command = JsonSerializer.SerializeToNode(config)
        };

        var result = await strategy.Build<string>("c1", command);

        Assert.Contains("FROM log.audit", result.Model);
        Assert.Contains("log_table='users'", result.Model);
    }
}
