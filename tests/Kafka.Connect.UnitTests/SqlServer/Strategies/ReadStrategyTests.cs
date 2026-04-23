using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.SqlServer.Models;
using Kafka.Connect.SqlServer.Strategies;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.SqlServer.Strategies;

public class ReadStrategyTests
{
    [Fact]
    public async Task BuildModels_WithConnectRecord_ThrowsNotImplementedException()
    {
        var sut = new ReadStrategy(Substitute.For<ILogger<ReadStrategy>>());
        await Assert.ThrowsAsync<NotImplementedException>(() =>
            sut.Build<string>("c1", new ConnectRecord("t", 0, 0)));
    }

    [Fact]
    public async Task BuildModels_SourceMode_ReturnsRowNumberQuery()
    {
        var sut = new ReadStrategy(Substitute.For<ILogger<ReadStrategy>>());
        var config = new CommandConfig
        {
            Schema = "dbo",
            Table = "orders",
            Filters = new Dictionary<string, object> { ["id"] = 0 }
        };
        var command = new CommandRecord
        {
            Command = JsonSerializer.SerializeToNode(config),
            BatchSize = 100
        };

        var result = await sut.Build<string>("c1", command);

        Assert.Equal(Status.Selecting, result.Status);
        Assert.Contains("ROW_NUMBER() OVER", result.Model);
        Assert.Contains("FETCH NEXT 100 ROWS ONLY", result.Model);
        Assert.Contains("[dbo].[orders]", result.Model);
    }

    [Fact]
    public async Task BuildModels_InitialSnapshot_ReturnsCountQuery()
    {
        var sut = new ReadStrategy(Substitute.For<ILogger<ReadStrategy>>());
        var config = new CommandConfig
        {
            Schema = "dbo",
            Table = "orders",
            Snapshot = new SnapshotConfig { Enabled = true, Total = 0 }
        };
        var command = new CommandRecord
        {
            Command = JsonSerializer.SerializeToNode(config),
            Changelog = JsonNode.Parse("{}"),
            BatchSize = 100
        };

        var result = await sut.Build<string>("c1", command);

        Assert.Equal(Status.Selecting, result.Status);
        Assert.Contains("COUNT(*) AS _total", result.Model);
        Assert.Contains("DATEDIFF_BIG", result.Model);
        Assert.Contains("[dbo].[orders]", result.Model);
    }

    [Fact]
    public async Task BuildModels_SnapshotBatch_ReturnsWindowedQuery()
    {
        var sut = new ReadStrategy(Substitute.For<ILogger<ReadStrategy>>());
        var config = new CommandConfig
        {
            Schema = "dbo",
            Table = "orders",
            Snapshot = new SnapshotConfig { Enabled = true, Total = 100, Id = 10 }
        };
        var command = new CommandRecord
        {
            Command = JsonSerializer.SerializeToNode(config),
            Changelog = JsonNode.Parse("{}"),
            BatchSize = 50
        };

        var result = await sut.Build<string>("c1", command);

        Assert.Equal(Status.Selecting, result.Status);
        Assert.Contains("WHERE id BETWEEN 11 AND 60", result.Model);
        Assert.Contains("'IMPORT' AS operation", result.Model);
    }

    [Fact]
    public async Task BuildModels_ChangelogMode_ReturnsAuditLogQuery()
    {
        var sut = new ReadStrategy(Substitute.For<ILogger<ReadStrategy>>());
        var config = new CommandConfig
        {
            Schema = "dbo",
            Table = "orders",
            Snapshot = new SnapshotConfig { Enabled = false, Timestamp = 1000, Id = 5 }
        };
        var changelog = new ChangelogConfig { Schema = "audit", Table = "log" };
        var command = new CommandRecord
        {
            Command = JsonSerializer.SerializeToNode(config),
            Changelog = JsonSerializer.SerializeToNode(changelog),
            BatchSize = 100
        };

        var result = await sut.Build<string>("c1", command);

        Assert.Equal(Status.Selecting, result.Status);
        Assert.Contains("log_before", result.Model);
        Assert.Contains("log_after", result.Model);
        Assert.Contains("[audit].[log]", result.Model);
    }
}
