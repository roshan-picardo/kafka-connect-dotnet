using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Kafka.Connect.Configurations;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Utilities;
using Microsoft.Extensions.Configuration;
using ConnectorConfig = Kafka.Connect.Configurations.ConnectorConfig;

namespace Kafka.Connect.Providers;

public class ConfigurationProvider : IConfigurationProvider, Kafka.Connect.Plugin.Providers.IConfigurationProvider
{
    private WorkerConfig _workerConfig;
    private LeaderConfig _leaderConfig;
    private IConfiguration _configuration;
    private const string DefaultConverter = "Kafka.Connect.Converters.AvroConverter";

    public ConfigurationProvider(IConfiguration configuration)
    {
        _configuration = configuration;
        SetWorkerConfig(configuration);
        SetLeaderConfig(configuration);
    }

    public bool IsLeader => _leaderConfig != null;
    public bool IsWorker => _workerConfig != null;

    public LeaderConfig GetLeaderConfig(bool reload = false)
    {
        if (reload)
        {
            var reloadedConfig = _configuration.ReloadConfigs(_leaderConfig.Settings);
            _configuration = reloadedConfig;
            SetLeaderConfig(reloadedConfig);
        }

        return _leaderConfig;
    }

    public WorkerConfig GetWorkerConfig() => _workerConfig;

    public void ReloadLeaderConfig()
    {
        var reloadedConfig = _configuration.ReloadConfigs(_leaderConfig.Settings);
        _configuration = reloadedConfig;
        SetLeaderConfig(reloadedConfig);
    }
    
    public void ReloadWorkerConfig()
    {
        if (_workerConfig?.Settings != null)
        {
            var reloadedConfig = _configuration.ReloadConfigs(_workerConfig.Settings);
            _configuration = reloadedConfig;
            SetWorkerConfig(reloadedConfig);
        }
        else if (IsLeader && _leaderConfig?.Settings != null)
        {
            var reloadedConfig = _configuration.ReloadConfigs(_leaderConfig.Settings);
            _configuration = reloadedConfig;
            SetWorkerConfig(reloadedConfig);
        }
    }
    
    private void SetLeaderConfig(IConfiguration config)
    {
        _leaderConfig = config.GetSection("leader").Get<LeaderConfig>();
        if (IsLeader)
        {
            _leaderConfig.Connectors.Clear();
            _leaderConfig.Connector.Topics =
                [_leaderConfig.Topics.TryGetValue(TopicType.Config, out var value) ? value : string.Empty];
            foreach (var section in config.GetSection("leader:connectors").GetChildren())
            {
                _leaderConfig.Connectors.Add(section.Key, section.ToJson());
            }
        }
    }

    private void SetWorkerConfig(IConfiguration config)
    {
        _workerConfig = config.GetSection("worker").Get<WorkerConfig>();
        if (IsWorker && !_workerConfig.Standalone)
        {
            _workerConfig.Connectors.Clear();
            _workerConfig.Connector.Topics =
                [_workerConfig.Topics.TryGetValue(TopicType.Config, out var value) ? value : string.Empty];
            foreach (var section in config.GetSection("worker:connectors").GetChildren())
            {
                var connectorConfig = section.Get<ConnectorConfig>();
                if (connectorConfig != null)
                {
                    connectorConfig.Name = section.Key;
                    _workerConfig.Connectors.Add(section.Key, connectorConfig);
                }
            }
        }
    }

    public FailOverConfig GetFailOverConfig()
    {
        NodeConfig config = IsLeader ? _leaderConfig : _workerConfig;
        return config.FailOver ?? new FailOverConfig();
    }
        
    public HealthCheckConfig GetHealthCheckConfig()
    {
        NodeConfig config = IsLeader ? _leaderConfig : _workerConfig;
        return config?.HealthCheck ??  new HealthCheckConfig();
    }
        
    public ConnectorConfig GetConnectorConfig(string connector)
    {
        if(IsLeader)
        {
            return _leaderConfig.Connector;
        }

        if (!_workerConfig.Standalone && _workerConfig.Connector.Name == connector)
        {
            return _workerConfig.Connector;
        }
        return _workerConfig.Connectors?.Values.SingleOrDefault(c => c.Name == connector) ??
               throw new ArgumentException($"{connector} isn't configured.");
    }

    public ConsumerConfig GetConsumerConfig(string connector = null)
    {
        if (IsLeader)
        {
            _leaderConfig.GroupId = _leaderConfig.Connector.GroupId;
            _leaderConfig.ClientId = _leaderConfig.Connector.GroupId;
            return _leaderConfig;
        }

        var config = GetConnectorConfig(connector);
        _workerConfig.GroupId = config.GroupId;
        _workerConfig.ClientId = config.ClientId;
        return _workerConfig;
    }

    public ProducerConfig GetProducerConfig(string connector = null)
    {
        var consumerConfig = GetConsumerConfig(connector) as NodeConfig;
        return new ProducerConfig(consumerConfig);
    }

    public IList<ConnectorConfig> GetAllConnectorConfigs(bool includeDisabled = false)
    {
        if (IsLeader)
        {
            return [_leaderConfig.Connector];
        }
        if (!_workerConfig.Standalone)
        {
            return [_workerConfig.Connector];
        }
        return _workerConfig.Connectors?.Values.Where(c => includeDisabled || !c.Disabled).ToList() ??
               throw new ArgumentException("Connectors aren't configured.");
    }

    public string GetNodeName()
    {
        return _workerConfig?.Name ?? _leaderConfig.Name;
    }

    public RestartsConfig GetRestartsConfig()
    {
        return _workerConfig?.Restarts ?? _leaderConfig?.Restarts ?? new RestartsConfig();
    }

    public ErrorsConfig GetErrorsConfig(string connector)
        => ((IsLeader ? _leaderConfig.FaultTolerance : _workerConfig.FaultTolerance) ??
            GetConnectorConfig(connector)?.FaultTolerance)?.Errors ?? new ErrorsConfig { Tolerance = ErrorTolerance.None };

    public RetryConfig GetRetriesConfig(string connector)
        => ((IsLeader ? _leaderConfig.FaultTolerance : _workerConfig.FaultTolerance) ??
            GetConnectorConfig(connector)?.FaultTolerance)?.Retries ?? new RetryConfig { Attempts = 3, Interval = 1000 };

    public EofConfig GetEofSignalConfig(string connector)
        => ((IsLeader ? _leaderConfig.FaultTolerance : _workerConfig.FaultTolerance) ??
            GetConnectorConfig(connector)?.FaultTolerance)?.Eof ?? new EofConfig();

    public BatchConfig GetBatchConfig(string connector)
    {
        NodeConfig nodeConfig = IsLeader ? _leaderConfig : _workerConfig;
        if (!(nodeConfig?.EnablePartitionEof ?? false))
        {
            return new BatchConfig { Size = 1, Parallelism = 1 };
        }

        return GetConnectorConfig(connector)?.FaultTolerance?.Batches
               ?? nodeConfig.FaultTolerance?.Batches
               ?? new BatchConfig { Size = 100, Parallelism = Environment.ProcessorCount, Interval = 5000 };
    }

    public string GetGroupId(string connector)
    {
        return GetConnectorConfig(connector).GroupId;
    }

    public string GetLogEnhancer(string connector)
    {
        return GetConnectorConfig(connector).Log?.Provider;
    }

    public string GetTopic(TopicType purpose)
    {
        var topics = IsLeader ? _leaderConfig.Topics : _workerConfig.Topics;
        return topics.TryGetValue(purpose, out var value) ? value : string.Empty;
    }

    public ConverterConfig GetMessageConverters(string connector, string topic)
    {
        var topicConfig = GetConnectorConfig(connector).Overrides?.SingleOrDefault(t => t.Key == topic).Value?.Converters;
        var connectorConfig = GetConnectorConfig(connector).Converters;
        var workerConfig = IsLeader ? _leaderConfig.Converters : _workerConfig.Converters;

        return new ConverterConfig
        {
            Key = topicConfig?.Key ?? connectorConfig?.Key ?? workerConfig?.Key ?? DefaultConverter,
            Value = topicConfig?.Value ?? connectorConfig?.Value ?? workerConfig?.Value ?? DefaultConverter,
            Subject = topicConfig?.Subject ?? connectorConfig?.Subject ?? workerConfig?.Subject ?? "Topic",
            Record = topicConfig?.Record ?? connectorConfig?.Record ?? workerConfig?.Record
        };
    }

    public IList<string> GetTopics(string connector)
    {
        if (IsLeader)
        {
            return [_leaderConfig.Topics.TryGetValue(TopicType.Config, out var config) ? config : string.Empty];
        }

        var connectorConfig = GetConnectorConfig(connector);
        return connectorConfig?.Plugin.Type == ConnectorType.Source
            ? [_workerConfig.Topics.TryGetValue(TopicType.Command, out var command) ? command : string.Empty]
            : GetConnectorConfig(connector).Topics;
    }

    public IList<ProcessorConfig> GetMessageProcessors(string connector, string topic)  
    {
        var workerConfigs = _workerConfig?.Processors ?? _leaderConfig?.Processors;
        var connectorConfigs = GetConnectorConfig(connector).Processors;
        var topicConfigs = GetConnectorConfig(connector).Overrides?.SingleOrDefault(t => t.Key == topic).Value?.Processors;

        var processors = new Dictionary<int, ProcessorConfig>();
        topicConfigs?.ForEach(config => processors.TryAdd(config.Key, config.Value));
        connectorConfigs?.ForEach(config => processors.TryAdd(config.Key, config.Value));
        workerConfigs?.ForEach(config => processors.TryAdd(config.Key, config.Value));

        return processors.OrderBy(p => p.Key).Select(p => p.Value).ToList();
    }

    public PluginConfig GetPluginConfig(string connector) => GetConnectorConfig(connector).Plugin ?? new PluginConfig();

    public bool IsErrorTolerated(string connector)
    {
        return GetErrorsConfig(connector).Tolerance == ErrorTolerance.All;
    }

    public bool IsDeadLetterEnabled(string connector)
    {
        var errors = GetErrorsConfig(connector);
        return errors.Tolerance != ErrorTolerance.None && !string.IsNullOrWhiteSpace(errors.Topic);
    }

    public (bool EnableAutoCommit, bool EnableAutoOffsetStore) GetAutoCommitConfig()
    {
        NodeConfig nodeConfig = IsLeader ? _leaderConfig : _workerConfig;
        return (nodeConfig.EnableAutoCommit ?? false, nodeConfig.EnableAutoOffsetStore ?? false);
    }

    public T GetProcessorSettings<T>(string connector, string processor)
    {
        var connectors = _configuration?.GetSection("worker:connectors")?.Get<IDictionary<string, ConnectorConfig<T>>>();
        var config = connectors?.SingleOrDefault(c => (c.Value.Name ?? c.Key) == connector).Value
            ?.Processors?.SingleOrDefault(p => p.Value != null && p.Value.Name == processor).Value;
        return config != null ? config.Settings : default;
    }

    public T GetPluginConfig<T>(string connector)
    {
        var connectors = _configuration?.GetSection("worker:connectors")?.Get<IDictionary<string, ConnectorPluginConfig<T>>>();
        if (connectors != null && connectors.TryGetValue(connector, out var config) && config?.Plugin != null)
        {
            return config.Plugin.Properties;
        }
        return default;
    }

    public  T GetLogAttributes<T>(string connector)
    {
        if (IsLeader)
        {
            return _leaderConfig.Connector.Log != null ? ((LogConfig<T>)_leaderConfig.Connector.Log).Attributes  : default;
        }
        var connectors = _configuration?.GetSection("worker:connectors")?.Get<IDictionary<string, ConnectorLogConfig<T>>>();
        var config = connectors?.SingleOrDefault(c => (c.Value.Name ?? c.Key) == connector).Value?.Log;
        return config == null ? default : config.Attributes;
    }

    public string GetPluginName(string connector)
    {
        return GetConnectorConfig(connector)?.Plugin.Name;
    }

    public InitializerConfig GetPlugin(string connector)
    {
        return
            _workerConfig.Plugins.Initializers.SingleOrDefault(p => p.Key == GetConnectorConfig(connector).Plugin.Name)
                .Value;
    }

    public int GetDegreeOfParallelism(string connector) => GetBatchConfig(connector).Parallelism;

    public ParallelRetryOptions GetParallelRetryOptions(string connector)
    {
        var batch = GetBatchConfig(connector);
        var retries = GetRetriesConfig(connector);
        var errorsConfig = GetErrorsConfig(connector);
        return new ParallelRetryOptions
        {
            DegreeOfParallelism = batch.Parallelism,
            Attempts = retries.Attempts,
            Interval = retries.Interval,
            ErrorTolerated = IsErrorTolerated(connector),
            ErrorTolerance = (
                All: errorsConfig.Tolerance == ErrorTolerance.All,
                Data: errorsConfig.Tolerance == ErrorTolerance.Data,
                None: errorsConfig.Tolerance == ErrorTolerance.None),
            Exceptions = errorsConfig.Exceptions ?? []
        };
    }

    public void Validate()  
    {
        if (string.IsNullOrWhiteSpace(_workerConfig.BootstrapServers))
        {
            throw new ArgumentException("Bootstrap Servers isn't configured for worker.");
        }

        if (_workerConfig.Standalone)
        {
            if (!(_workerConfig.Connectors?.Any() ?? false))
            {
                throw new ArgumentException("At least one connector is required for the worker to start.");
            }

            var hash = new HashSet<string>();
            hash.Clear();
            if (!_workerConfig.Connectors.Values.All(c =>
                    c != null && hash.Add(c.Name) && !string.IsNullOrEmpty(c.Name)))
            {
                throw new ArgumentException(
                    "Connector Name configuration property must be specified and must be unique.");
            }

            if (!(_workerConfig.Plugins?.Initializers?.Any() ?? false))
            {
                throw new ArgumentException("At least one plugin is required for the worker to start.");
            }

            hash.Clear();
            if (!_workerConfig.Plugins.Initializers.All(p => hash.Add(p.Key) && !string.IsNullOrEmpty(p.Key)))
            {
                throw new ArgumentException("Plugin Name configuration property must be specified and must be unique.");
            }

            foreach (var connector in _workerConfig.Connectors?.Values ?? new List<ConnectorConfig>())
            {
                if (!_workerConfig.Plugins.Initializers.Select(p => p.Key).Contains(connector.Plugin.Name))
                {
                    throw new ArgumentException(
                        $"Connector: {connector.Name} is not associated to any of the available Plugins: [ {string.Join(", ", _workerConfig.Plugins.Initializers.Select(p => p.Key))} ].");
                }
            }
        }
    }
}
