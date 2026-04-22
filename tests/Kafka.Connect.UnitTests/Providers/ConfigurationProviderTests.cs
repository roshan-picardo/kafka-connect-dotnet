using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Kafka.Connect.Configurations;
using Microsoft.Extensions.Configuration;
using Xunit;
using ConfigurationProvider = Kafka.Connect.Providers.ConfigurationProvider;

namespace UnitTests.Kafka.Connect.Providers;

public class ConfigurationProviderTests
{
    [Fact]
    public void WorkerConfiguration_LoadsDistributedConnectorState()
    {
        var provider = CreateProvider(BaseWorkerSettings());

        Assert.True(provider.IsWorker);
        Assert.False(provider.IsLeader);
        Assert.False(provider.GetWorkerConfig().Standalone);
        Assert.Equal("worker-a", provider.GetNodeName());

        var connector = provider.GetConnectorConfig("orders");
        Assert.Equal("orders", connector.Name);
        Assert.Equal("orders-group", provider.GetGroupId("orders"));
        Assert.Equal("custom.log.provider", provider.GetLogEnhancer("orders"));
        Assert.Equal("plugin-a", provider.GetPluginName("orders"));
        Assert.Equal("command-topic", provider.GetTopic(TopicType.Command));
    }

    [Fact]
    public void LeaderConfiguration_LoadsLeaderConnectorAndTopics()
    {
        var provider = CreateProvider(new Dictionary<string, string>
        {
            ["leader:name"] = "leader-a",
            ["leader:bootstrapServers"] = "localhost:9092",
            ["leader:groupId"] = "leader-group",
            ["leader:topics:Config"] = "leader-config-topic",
            ["leader:connectors:alpha:tasks"] = "1"
        });

        Assert.True(provider.IsLeader);
        Assert.False(provider.IsWorker);
        Assert.Equal("leader-a", provider.GetNodeName());
        Assert.Equal("leader-config-topic", provider.GetTopics("ignored").Single());
        Assert.Equal(ConnectorType.Leader, provider.GetConnectorConfig("ignored").Plugin.Type);
        Assert.Single(provider.GetAllConnectorConfigs());
        Assert.True(provider.GetLeaderConfig().Connectors.ContainsKey("alpha"));
        Assert.Equal("alpha", provider.GetLeaderConfig().Connectors["alpha"]?["name"]?.GetValue<string>());
    }

    [Fact]
    public void GetMessageConverters_UsesTopicThenConnectorThenWorkerFallbacks()
    {
        var provider = CreateProvider(BaseWorkerSettings());

        var actual = provider.GetMessageConverters("orders", "topic-a");

        Assert.Equal("ConnectorKeyConverter", actual.Key);
        Assert.Equal("TopicValueConverter", actual.Value);
        Assert.Equal("WorkerSubject", actual.Subject);
        Assert.Null(actual.Record);
    }

    [Fact]
    public void GetMessageConverters_UsesDefaultsWhenNothingConfigured()
    {
        var provider = CreateProvider(new Dictionary<string, string>
        {
            ["worker:name"] = "worker-a",
            ["worker:bootstrapServers"] = "localhost:9092",
            ["worker:standalone"] = "true",
            ["worker:connectors:orders:name"] = "orders",
            ["worker:connectors:orders:plugin:name"] = "plugin-a",
            ["worker:connectors:orders:plugin:type"] = "Sink",
            ["worker:connectors:orders:topics:0"] = "orders-topic"
        });

        var actual = provider.GetMessageConverters("orders", "orders-topic");

        Assert.Equal("Kafka.Connect.Converters.AvroConverter", actual.Key);
        Assert.Equal("Kafka.Connect.Converters.AvroConverter", actual.Value);
        Assert.Equal("Topic", actual.Subject);
        Assert.Null(actual.Record);
    }

    [Fact]
    public void GetMessageProcessors_MergesTopicConnectorAndWorkerByOrder()
    {
        var provider = CreateProvider(BaseWorkerSettings());

        var actual = provider.GetMessageProcessors("orders", "topic-a");

        Assert.Equal(4, actual.Count);
        Assert.Equal(new[] { "TopicProc0", "ConnectorProc1", "TopicProc2", "ProcSettings" }, actual.Select(a => a.Name).ToArray());
    }

    [Fact]
    public void FaultToleranceMethods_ComposeCurrentFallbackBehavior()
    {
        var provider = CreateProvider(BaseWorkerSettings());

        var errors = provider.GetErrorsConfig("orders");
        var retries = provider.GetRetriesConfig("orders");
        var eof = provider.GetEofSignalConfig("orders");
        var batch = provider.GetBatchConfig("orders");
        var parallel = provider.GetParallelRetryOptions("orders");

        Assert.Equal(ErrorTolerance.Data, errors.Tolerance);
        Assert.Equal("worker-dlq", errors.Topic);
        Assert.Equal(4, retries.Attempts);
        Assert.Equal(250, retries.Interval);
        Assert.True(eof.Enabled);
        Assert.Equal("worker-eof", eof.Topic);
        Assert.Equal(10, batch.Size);
        Assert.Equal(3, batch.Parallelism);
        Assert.False(provider.IsErrorTolerated("orders"));
        Assert.True(provider.IsDeadLetterEnabled("orders"));
        Assert.Equal(3, provider.GetDegreeOfParallelism("orders"));
        Assert.Equal(3, parallel.DegreeOfParallelism);
        Assert.Equal(4, parallel.Attempts);
        Assert.Equal(250, parallel.Interval);
        Assert.True(parallel.ErrorTolerance.Data);
        Assert.Contains("System.Exception", parallel.Exceptions);
    }

    [Fact]
    public void GetBatchConfig_WhenPartitionEofDisabled_ReturnsSingleRecordDefaults()
    {
        var provider = CreateProvider(new Dictionary<string, string>
        {
            ["worker:name"] = "worker-a",
            ["worker:bootstrapServers"] = "localhost:9092",
            ["worker:standalone"] = "true",
            ["worker:connectors:orders:name"] = "orders",
            ["worker:connectors:orders:plugin:name"] = "plugin-a",
            ["worker:connectors:orders:plugin:type"] = "Sink"
        });

        var actual = provider.GetBatchConfig("orders");

        Assert.Equal(1, actual.Size);
        Assert.Equal(1, actual.Parallelism);
    }

    [Fact]
    public void GetTopics_ForSourceConnector_UsesCommandTopic()
    {
        var values = BaseWorkerSettings();
        values["worker:connectors:orders:plugin:type"] = "Source";
        values["worker:connectors:orders:topics:0"] = "should-not-be-used";
        var provider = CreateProvider(values);

        var actual = provider.GetTopics("orders");

        Assert.Equal(["command-topic"], actual);
    }

    [Fact]
    public void GetGenericSettingsAndPluginMetadata_ReadFromConfiguration()
    {
        var provider = CreateProvider(BaseWorkerSettings());

        var processorSettings = provider.GetProcessorSettings<Dictionary<string, string>>("orders", "ProcSettings");
        var pluginProperties = provider.GetPluginConfig<Dictionary<string, string>>("orders");
        var logAttributes = provider.GetLogAttributes<string[]>("orders");
        var plugin = provider.GetPlugin("orders");

        Assert.Equal("true", processorSettings["enabled"]);
        Assert.Equal("full", pluginProperties["mode"]);
        Assert.Equal(new[] { "fieldA", "items[*]" }, logAttributes);
        Assert.Equal("plugin-a", provider.GetPluginName("orders"));
        Assert.Equal("plugin-a", plugin.Folder);
        Assert.Equal("Plugin.dll", plugin.Assembly);
    }

    [Fact]
    public void GetAutoCommitConfig_ReturnsWorkerFlags()
    {
        var provider = CreateProvider(BaseWorkerSettings());

        var actual = provider.GetAutoCommitConfig();

        Assert.True(actual.EnableAutoCommit);
        Assert.False(actual.EnableAutoOffsetStore);
    }

    [Fact]
    public void ReloadWorkerConfig_UsesSettingsFolderJsonFiles()
    {
        var tempDir = CreateTempDirectory();
        try
        {
            File.WriteAllText(Path.Combine(tempDir, "worker.override.json"), """
            {
              "worker": {
                "name": "reloaded-worker"
              }
            }
            """);

            var provider = CreateProvider(new Dictionary<string, string>
            {
                ["worker:name"] = "initial-worker",
                ["worker:settings"] = tempDir,
                ["worker:bootstrapServers"] = "localhost:9092",
                ["worker:standalone"] = "true",
                ["worker:connectors:orders:name"] = "orders",
                ["worker:connectors:orders:plugin:name"] = "plugin-a",
                ["worker:connectors:orders:plugin:type"] = "Sink"
            });

            provider.ReloadWorkerConfig();

            Assert.Equal("reloaded-worker", provider.GetNodeName());
        }
        finally
        {
            Directory.Delete(tempDir, true);
        }
    }

    [Fact]
    public void GetLeaderConfig_WithReload_UsesSettingsFolderJsonFiles()
    {
        var tempDir = CreateTempDirectory();
        try
        {
            File.WriteAllText(Path.Combine(tempDir, "leader.override.json"), """
            {
              "leader": {
                "name": "reloaded-leader"
              }
            }
            """);

            var provider = CreateProvider(new Dictionary<string, string>
            {
                ["leader:name"] = "initial-leader",
                ["leader:settings"] = tempDir,
                ["leader:bootstrapServers"] = "localhost:9092",
                ["leader:topics:Config"] = "leader-config-topic"
            });

            var actual = provider.GetLeaderConfig(reload: true);

            Assert.Equal("reloaded-leader", actual.Name);
        }
        finally
        {
            Directory.Delete(tempDir, true);
        }
    }

    [Fact]
    public void GetAllConnectorConfigs_WhenStandalone_RespectsDisabledFlag()
    {
        var provider = CreateProvider(new Dictionary<string, string>
        {
            ["worker:name"] = "worker-a",
            ["worker:bootstrapServers"] = "localhost:9092",
            ["worker:standalone"] = "true",
            ["worker:plugins:initializers:plugin-a:folder"] = "plugin-a",
            ["worker:plugins:initializers:plugin-a:assembly"] = "Plugin.dll",
            ["worker:plugins:initializers:plugin-a:class"] = "Plugin.Initializer",
            ["worker:connectors:enabled:name"] = "enabled",
            ["worker:connectors:enabled:plugin:name"] = "plugin-a",
            ["worker:connectors:enabled:plugin:type"] = "Sink",
            ["worker:connectors:disabled:name"] = "disabled",
            ["worker:connectors:disabled:disabled"] = "true",
            ["worker:connectors:disabled:plugin:name"] = "plugin-a",
            ["worker:connectors:disabled:plugin:type"] = "Sink"
        });

        Assert.Single(provider.GetAllConnectorConfigs());
        Assert.Equal(2, provider.GetAllConnectorConfigs(includeDisabled: true).Count);
    }

    [Fact]
    public void Validate_ThrowsWhenStandaloneWorkerHasNoConnectors()
    {
        var provider = CreateProvider(new Dictionary<string, string>
        {
            ["worker:name"] = "worker-a",
            ["worker:bootstrapServers"] = "localhost:9092",
            ["worker:standalone"] = "true"
        });

        var exception = Assert.Throws<ArgumentException>(() => provider.Validate());

        Assert.Contains("At least one connector is required", exception.Message);
    }

    [Fact]
    public void Validate_ThrowsWhenConnectorPluginIsUnavailable()
    {
        var provider = CreateProvider(new Dictionary<string, string>
        {
            ["worker:name"] = "worker-a",
            ["worker:bootstrapServers"] = "localhost:9092",
            ["worker:standalone"] = "true",
            ["worker:plugins:initializers:plugin-a:folder"] = "plugin-a",
            ["worker:plugins:initializers:plugin-a:assembly"] = "Plugin.dll",
            ["worker:plugins:initializers:plugin-a:class"] = "Plugin.Initializer",
            ["worker:connectors:orders:name"] = "orders",
            ["worker:connectors:orders:plugin:name"] = "missing-plugin",
            ["worker:connectors:orders:plugin:type"] = "Sink"
        });

        var exception = Assert.Throws<ArgumentException>(() => provider.Validate());

        Assert.Contains("is not associated to any of the available Plugins", exception.Message);
    }

    [Fact]
    public void Validate_DoesNotThrowForValidStandaloneConfiguration()
    {
        var provider = CreateProvider(new Dictionary<string, string>
        {
            ["worker:name"] = "worker-a",
            ["worker:bootstrapServers"] = "localhost:9092",
            ["worker:standalone"] = "true",
            ["worker:plugins:initializers:plugin-a:folder"] = "plugin-a",
            ["worker:plugins:initializers:plugin-a:assembly"] = "Plugin.dll",
            ["worker:plugins:initializers:plugin-a:class"] = "Plugin.Initializer",
            ["worker:connectors:orders:name"] = "orders",
            ["worker:connectors:orders:plugin:name"] = "plugin-a",
            ["worker:connectors:orders:plugin:type"] = "Sink"
        });

        provider.Validate();
    }

    private static ConfigurationProvider CreateProvider(IDictionary<string, string> values)
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(values)
            .Build();

        return new ConfigurationProvider(configuration);
    }

    private static Dictionary<string, string> BaseWorkerSettings()
    {
        return new Dictionary<string, string>
        {
            ["worker:name"] = "worker-a",
            ["worker:bootstrapServers"] = "localhost:9092",
            ["worker:standalone"] = "false",
            ["worker:enablePartitionEof"] = "true",
            ["worker:enableAutoCommit"] = "true",
            ["worker:enableAutoOffsetStore"] = "false",
            ["worker:topics:Command"] = "command-topic",
            ["worker:topics:Config"] = "config-topic",
            ["worker:converters:key"] = "WorkerKeyConverter",
            ["worker:converters:value"] = "WorkerValueConverter",
            ["worker:converters:subject"] = "WorkerSubject",
            ["worker:processors:2:name"] = "WorkerProc2",
            ["worker:faultTolerance:batches:size"] = "50",
            ["worker:faultTolerance:batches:parallelism"] = "5",
            ["worker:faultTolerance:retries:attempts"] = "4",
            ["worker:faultTolerance:retries:interval"] = "250",
            ["worker:faultTolerance:errors:tolerance"] = "Data",
            ["worker:faultTolerance:errors:topic"] = "worker-dlq",
            ["worker:faultTolerance:errors:exceptions:0"] = "System.Exception",
            ["worker:faultTolerance:eof:enabled"] = "true",
            ["worker:faultTolerance:eof:topic"] = "worker-eof",
            ["worker:plugins:initializers:plugin-a:folder"] = "plugin-a",
            ["worker:plugins:initializers:plugin-a:assembly"] = "Plugin.dll",
            ["worker:plugins:initializers:plugin-a:class"] = "Plugin.Initializer",
            ["worker:connectors:orders:name"] = "orders",
            ["worker:connectors:orders:groupId"] = "orders-group",
            ["worker:connectors:orders:clientId"] = "orders-client",
            ["worker:connectors:orders:topics:0"] = "orders-topic",
            ["worker:connectors:orders:plugin:name"] = "plugin-a",
            ["worker:connectors:orders:plugin:type"] = "Sink",
            ["worker:connectors:orders:plugin:handler"] = "handler-a",
            ["worker:connectors:orders:plugin:properties:mode"] = "full",
            ["worker:connectors:orders:converters:key"] = "ConnectorKeyConverter",
            ["worker:connectors:orders:log:provider"] = "custom.log.provider",
            ["worker:connectors:orders:log:attributes:0"] = "fieldA",
            ["worker:connectors:orders:log:attributes:1"] = "items[*]",
            ["worker:connectors:orders:processors:1:name"] = "ConnectorProc1",
            ["worker:connectors:orders:processors:2:name"] = "ConnectorProc2",
            ["worker:connectors:orders:processors:5:name"] = "ProcSettings",
            ["worker:connectors:orders:processors:5:settings:enabled"] = "true",
            ["worker:connectors:orders:overrides:topic-a:converters:value"] = "TopicValueConverter",
            ["worker:connectors:orders:overrides:topic-a:processors:0:name"] = "TopicProc0",
            ["worker:connectors:orders:overrides:topic-a:processors:2:name"] = "TopicProc2",
            ["worker:connectors:orders:faultTolerance:batches:size"] = "10",
            ["worker:connectors:orders:faultTolerance:batches:parallelism"] = "3",
            ["worker:connectors:orders:faultTolerance:retries:attempts"] = "7",
            ["worker:connectors:orders:faultTolerance:retries:interval"] = "300",
            ["worker:connectors:orders:faultTolerance:errors:tolerance"] = "All",
            ["worker:connectors:orders:faultTolerance:errors:topic"] = "connector-dlq",
            ["worker:connectors:orders:faultTolerance:errors:exceptions:0"] = "System.InvalidOperationException",
            ["worker:connectors:orders:faultTolerance:eof:enabled"] = "true",
            ["worker:connectors:orders:faultTolerance:eof:topic"] = "connector-eof"
        };
    }

    private static string CreateTempDirectory()
    {
        var path = Path.Combine(Path.GetTempPath(), "kafka-connect-provider-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }
}
