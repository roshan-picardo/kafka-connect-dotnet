using System.Collections.Generic;
using System.Text.Json.Nodes;
using Kafka.Connect.Configurations;
using Kafka.Connect.Providers;
using Xunit;

namespace UnitTests.Kafka.Connect.Configurations;

// ── ConnectorConfig ───────────────────────────────────────────────────────────

public class ConnectorConfigTests
{
    [Fact]
    public void GroupId_WhenNotSet_FallsBackToName()
    {
        var config = new ConnectorConfig { Name = "my-connector" };
        Assert.Equal("my-connector", config.GroupId);
    }

    [Fact]
    public void GroupId_WhenSet_ReturnsExplicitValue()
    {
        var config = new ConnectorConfig { Name = "my-connector", GroupId = "explicit-group" };
        Assert.Equal("explicit-group", config.GroupId);
    }

    [Fact]
    public void ClientId_WhenNotSet_FallsBackToName()
    {
        var config = new ConnectorConfig { Name = "my-connector" };
        Assert.Equal("my-connector", config.ClientId);
    }

    [Fact]
    public void ClientId_WhenSet_ReturnsExplicitValue()
    {
        var config = new ConnectorConfig { Name = "my-connector", ClientId = "explicit-client" };
        Assert.Equal("explicit-client", config.ClientId);
    }

    [Fact]
    public void Log_WhenNotSet_ReturnsDefaultLogConfigWithDefaultLogRecordProvider()
    {
        var config = new ConnectorConfig { Name = "my-connector" };
        Assert.NotNull(config.Log);
        Assert.Equal(typeof(DefaultLogRecord).FullName, config.Log.Provider);
    }

    [Fact]
    public void Log_WhenSet_ReturnsExplicitLogConfig()
    {
        var logConfig = new LogConfig { Provider = "custom-provider" };
        var config = new ConnectorConfig { Log = logConfig };
        Assert.Equal("custom-provider", config.Log.Provider);
    }
}

// ── LogConfig ─────────────────────────────────────────────────────────────────

public class LogConfigTests
{
    [Fact]
    public void Provider_WhenNotSet_ReturnsDefaultLogRecordFullName()
    {
        var config = new LogConfig();
        Assert.Equal(typeof(DefaultLogRecord).FullName, config.Provider);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Provider_WhenNullOrWhitespace_ReturnsDefaultLogRecordFullName(string provider)
    {
        var config = new LogConfig { Provider = provider };
        Assert.Equal(typeof(DefaultLogRecord).FullName, config.Provider);
    }

    [Fact]
    public void Provider_WhenSet_ReturnsExplicitValue()
    {
        var config = new LogConfig { Provider = "my.log.provider" };
        Assert.Equal("my.log.provider", config.Provider);
    }
}

// ── RestartsConfig ────────────────────────────────────────────────────────────

public class RestartsConfigTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-9999)]
    public void PeriodicDelayMs_WhenZeroOrNegative_ReturnsDefault(int value)
    {
        var config = new RestartsConfig { PeriodicDelayMs = value };
        Assert.Equal(2000, config.PeriodicDelayMs);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(1000)]
    [InlineData(5000)]
    public void PeriodicDelayMs_WhenPositive_ReturnsSetValue(int value)
    {
        var config = new RestartsConfig { PeriodicDelayMs = value };
        Assert.Equal(value, config.PeriodicDelayMs);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-9999)]
    public void RetryWaitTimeMs_WhenZeroOrNegative_ReturnsDefault(int value)
    {
        var config = new RestartsConfig { RetryWaitTimeMs = value };
        Assert.Equal(30000, config.RetryWaitTimeMs);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(5000)]
    [InlineData(60000)]
    public void RetryWaitTimeMs_WhenPositive_ReturnsSetValue(int value)
    {
        var config = new RestartsConfig { RetryWaitTimeMs = value };
        Assert.Equal(value, config.RetryWaitTimeMs);
    }
}

// ── FailOverConfig ────────────────────────────────────────────────────────────

public class FailOverConfigTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void InitialDelayMs_WhenZeroOrNegative_ReturnsDefault(int value)
    {
        var config = new FailOverConfig { InitialDelayMs = value };
        Assert.Equal(60000, config.InitialDelayMs);
    }

    [Fact]
    public void InitialDelayMs_WhenPositive_ReturnsSetValue()
    {
        var config = new FailOverConfig { InitialDelayMs = 5000 };
        Assert.Equal(5000, config.InitialDelayMs);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void PeriodicDelayMs_WhenZeroOrNegative_ReturnsDefault(int value)
    {
        var config = new FailOverConfig { PeriodicDelayMs = value };
        Assert.Equal(10000, config.PeriodicDelayMs);
    }

    [Fact]
    public void PeriodicDelayMs_WhenPositive_ReturnsSetValue()
    {
        var config = new FailOverConfig { PeriodicDelayMs = 3000 };
        Assert.Equal(3000, config.PeriodicDelayMs);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void FailureThreshold_WhenZeroOrNegative_ReturnsDefault(int value)
    {
        var config = new FailOverConfig { FailureThreshold = value };
        Assert.Equal(20, config.FailureThreshold);
    }

    [Fact]
    public void FailureThreshold_WhenPositive_ReturnsSetValue()
    {
        var config = new FailOverConfig { FailureThreshold = 5 };
        Assert.Equal(5, config.FailureThreshold);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void RestartDelayMs_WhenZeroOrNegative_ReturnsDefault(int value)
    {
        var config = new FailOverConfig { RestartDelayMs = value };
        Assert.Equal(10000, config.RestartDelayMs);
    }

    [Fact]
    public void RestartDelayMs_WhenPositive_ReturnsSetValue()
    {
        var config = new FailOverConfig { RestartDelayMs = 2000 };
        Assert.Equal(2000, config.RestartDelayMs);
    }
}

// ── WorkerConfig ──────────────────────────────────────────────────────────────

public class WorkerConfigTests
{
    [Fact]
    public void Connectors_InitWithNull_ReturnsEmptyDictionary()
    {
        var config = new WorkerConfig { Connectors = null };
        Assert.NotNull(config.Connectors);
        Assert.Empty(config.Connectors);
    }

    [Fact]
    public void Connectors_WhenEmpty_ReturnsEmpty()
    {
        var config = new WorkerConfig { Connectors = new Dictionary<string, ConnectorConfig>() };
        Assert.Empty(config.Connectors);
    }

    [Fact]
    public void Connectors_WhenConnectorHasNoName_AutoPopulatesFromKey()
    {
        var config = new WorkerConfig
        {
            Connectors = new Dictionary<string, ConnectorConfig>
            {
                ["auto-named"] = new ConnectorConfig()
            }
        };
        Assert.Equal("auto-named", config.Connectors["auto-named"].Name);
    }

    [Fact]
    public void Connectors_WhenConnectorAlreadyHasName_DoesNotOverride()
    {
        var config = new WorkerConfig
        {
            Connectors = new Dictionary<string, ConnectorConfig>
            {
                ["key-name"] = new ConnectorConfig { Name = "explicit-name" }
            }
        };
        Assert.Equal("explicit-name", config.Connectors["key-name"].Name);
    }

    [Fact]
    public void Connectors_WhenConnectorIsNull_SkipsWithoutException()
    {
        var config = new WorkerConfig
        {
            Connectors = new Dictionary<string, ConnectorConfig>
            {
                ["null-entry"] = null
            }
        };
        // Should not throw; null entries are skipped
        _ = config.Connectors;
    }

    [Fact]
    public void Connector_ReturnsWorkerPluginType()
    {
        var config = new WorkerConfig
        {
            Name = "worker-node",
            Topics = new Dictionary<TopicType, string>()
        };
        Assert.Equal(ConnectorType.Worker, config.Connector.Plugin.Type);
    }

    [Fact]
    public void Connector_WhenConfigTopicSet_PopulatesTopics()
    {
        var config = new WorkerConfig
        {
            Name = "worker-node",
            Topics = new Dictionary<TopicType, string>
            {
                [TopicType.Config] = "config-topic"
            }
        };
        Assert.Contains("config-topic", config.Connector.Topics);
    }

    [Fact]
    public void Connector_WhenNoConfigTopic_TopicsIsNull()
    {
        var config = new WorkerConfig
        {
            Name = "worker-node",
            Topics = new Dictionary<TopicType, string>()
        };
        Assert.Null(config.Connector.Topics);
    }

    [Fact]
    public void Connector_InheritsNameAndGroupId()
    {
        var config = new WorkerConfig
        {
            Name = "worker-node",
            Topics = new Dictionary<TopicType, string>(),
            GroupId = "worker-group"
        };
        Assert.Equal("worker-node", config.Connector.Name);
        Assert.Equal("worker-group", config.Connector.GroupId);
    }
}

// ── LeaderConfig ──────────────────────────────────────────────────────────────

public class LeaderConfigTests
{
    [Fact]
    public void Connectors_WhenEmpty_ReturnsEmpty()
    {
        var config = new LeaderConfig { Connectors = new Dictionary<string, JsonNode>() };
        Assert.Empty(config.Connectors);
    }

    [Fact]
    public void Connectors_WhenConnectorJsonHasNoName_AutoPopulatesFromKey()
    {
        var node = JsonNode.Parse("""{}""");
        var config = new LeaderConfig
        {
            Connectors = new Dictionary<string, JsonNode> { ["auto-named"] = node }
        };
        Assert.Equal("auto-named", config.Connectors["auto-named"]["name"]?.GetValue<string>());
    }

    [Fact]
    public void Connectors_WhenConnectorJsonAlreadyHasName_DoesNotOverride()
    {
        var node = JsonNode.Parse("""{"name":"explicit-name"}""");
        var config = new LeaderConfig
        {
            Connectors = new Dictionary<string, JsonNode> { ["key-name"] = node }
        };
        Assert.Equal("explicit-name", config.Connectors["key-name"]["name"]?.GetValue<string>());
    }

    [Fact]
    public void Connectors_WhenConnectorIsNull_SkipsWithoutException()
    {
        var config = new LeaderConfig
        {
            Connectors = new Dictionary<string, JsonNode> { ["null-entry"] = null }
        };
        _ = config.Connectors;
    }

    [Fact]
    public void GetConfigurationTopic_WhenConfigTopicExists_ReturnsTopic()
    {
        var config = new LeaderConfig
        {
            Topics = new Dictionary<TopicType, string>
            {
                [TopicType.Config] = "leader-config-topic"
            }
        };
        Assert.Equal("leader-config-topic", config.GetConfigurationTopic());
    }

    [Fact]
    public void GetConfigurationTopic_WhenNoConfigTopic_ReturnsEmpty()
    {
        var config = new LeaderConfig
        {
            Topics = new Dictionary<TopicType, string>()
        };
        Assert.Equal(string.Empty, config.GetConfigurationTopic());
    }

    [Fact]
    public void Connector_ReturnsLeaderPluginType()
    {
        var config = new LeaderConfig
        {
            Name = "leader-node",
            Topics = new Dictionary<TopicType, string>()
        };
        Assert.Equal(ConnectorType.Leader, config.Connector.Plugin.Type);
    }

    [Fact]
    public void Connector_HasTwoTasks()
    {
        var config = new LeaderConfig
        {
            Name = "leader-node",
            Topics = new Dictionary<TopicType, string>()
        };
        Assert.Equal(2, config.Connector.Tasks);
    }

    [Fact]
    public void Connector_InheritsNameAndGroupId()
    {
        var config = new LeaderConfig
        {
            Name = "leader-node",
            Topics = new Dictionary<TopicType, string>(),
            GroupId = "leader-group"
        };
        Assert.Equal("leader-node", config.Connector.Name);
        Assert.Equal("leader-group", config.Connector.GroupId);
    }
}
