using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Handlers;

public sealed class ConfigurationChangeHandlerTests : IDisposable
{
    private readonly IConfigurationProvider _configurationProvider = Substitute.For<IConfigurationProvider>();
    private readonly ConfigurationChangeHandler _handler;
    private readonly string _tempDir;

    public ConfigurationChangeHandlerTests()
    {
        _handler = new ConfigurationChangeHandler(
            _configurationProvider,
            Substitute.For<ILogger<ConfigurationChangeHandler>>());

        _tempDir = Path.Combine(Path.GetTempPath(), "kconnect-tests", Guid.NewGuid().ToString("N"));
    }

    [Fact]
    public async Task Store_LeaderMode_WritesRecordsDeletesEmptyAndRefreshesOrphans()
    {
        Directory.CreateDirectory(_tempDir);
        var orphanPath = Path.Combine(_tempDir, "orphan.json");
        var deletePath = Path.Combine(_tempDir, "delete-me.json");
        await File.WriteAllTextAsync(orphanPath, "{}");
        await File.WriteAllTextAsync(deletePath, "{}");

        _configurationProvider.GetLeaderConfig(true).Returns(new LeaderConfig { Settings = _tempDir });

        var records = new[]
        {
            BuildRecord("active", JsonNode.Parse("{\"connector\":{\"tasks\":1}}"), 1),
            BuildRecord("delete-me", JsonNode.Parse("{}"), 2)
        };

        await _handler.Store(records, refresh: true);

        var activePath = Path.Combine(_tempDir, "active.json");
        Assert.True(File.Exists(activePath));
        Assert.False(File.Exists(deletePath));
        Assert.False(File.Exists(orphanPath));
        Assert.Equal(Status.Saved, records[0].Status);
        Assert.Equal(Status.Deleted, records[1].Status);
    }

    [Fact]
    public async Task Store_WorkerMode_UsesLatestOffsetAndDeletesWhenWorkerNotAssigned()
    {
        Directory.CreateDirectory(_tempDir);
        var existingPath = Path.Combine(_tempDir, "orders.json");
        await File.WriteAllTextAsync(existingPath, "{\"old\":true}");

        _configurationProvider.GetWorkerConfig().Returns(new WorkerConfig { Settings = _tempDir });

        var older = BuildRecord("orders", JsonNode.Parse("{\"workers\":[\"worker-a\"],\"connector\":{\"tasks\":1}}"), 10);
        var latest = BuildRecord("orders", JsonNode.Parse("{\"workers\":[\"worker-b\"],\"connector\":{\"tasks\":2}}"), 11);

        await _handler.Store(new[] { older, latest }, "worker-a");

        // Latest record excludes worker-a, so file must be deleted.
        Assert.False(File.Exists(existingPath));
        Assert.Equal(Status.Empty, older.Status);
        Assert.Equal(Status.Deleted, latest.Status);
    }

    [Fact]
    public async Task Store_WorkerMode_WritesWrappedWorkerConfig_WhenWorkerAssigned()
    {
        _configurationProvider.GetWorkerConfig().Returns(new WorkerConfig { Settings = _tempDir });

        var record = BuildRecord(
            "billing",
            JsonNode.Parse("{\"workers\":[\"worker-a\"],\"connector\":{\"tasks\":3}}"),
            1);

        await _handler.Store(new[] { record }, "worker-a");

        var path = Path.Combine(_tempDir, "billing.json");
        Assert.True(File.Exists(path));

        var saved = JsonNode.Parse(await File.ReadAllTextAsync(path));
        Assert.Equal(3, saved?["worker"]?["connectors"]?["connector"]?["tasks"]?.GetValue<int>());
        Assert.Null(saved?["worker"]?["workers"]);
        Assert.Equal(Status.Saved, record.Status);
    }

    [Fact]
    public void Configure_TransformsConnectorPropertyAndBuildsConnectRecord()
    {
        _configurationProvider.GetLeaderConfig().Returns(new LeaderConfig
        {
            Topics = new Dictionary<TopicType, string> { [TopicType.Config] = "config-topic" }
        });

        var settings = JsonNode.Parse("{\"connector\":{\"tasks\":2},\"workers\":[\"a\"]}")?.AsObject();

        var record = _handler.Configure("orders", settings);

        Assert.Equal("config-topic", record.Topic);
        Assert.Equal("orders", record.GetKey<string>());
        Assert.Null(record.GetValue<JsonNode>()?["connector"]);
        Assert.Equal(2, record.GetValue<JsonNode>()?["orders"]?["tasks"]?.GetValue<int>());
        Assert.Equal(Status.Selected, record.Status);
    }

    private static ConnectRecord BuildRecord(string key, JsonNode value, long offset)
    {
        return new ConnectRecord("config-topic", 0, offset)
        {
            Deserialized = new ConnectMessage<JsonNode>
            {
                Key = JsonValue.Create(key),
                Value = value
            }
        };
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, true);
        }
    }
}
