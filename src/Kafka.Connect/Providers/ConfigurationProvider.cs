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
    private readonly WorkerConfig _workerConfig;
    private LeaderConfig _leaderConfig;
    private readonly IConfiguration _configuration;
    private const string DefaultConverter = "Kafka.Connect.Serializers.AvroDeserializer";

    public ConfigurationProvider(IConfiguration configuration)
    {
        _configuration = configuration;
        _workerConfig = configuration.GetSection("worker").Get<WorkerConfig>();
        SetLeaderConfig(configuration);
    }

    public bool IsLeader => _leaderConfig != null;
    public bool IsWorker => _workerConfig != null;

    public LeaderConfig GetLeaderConfig(bool reload = false)
    {
        if (reload)
        {
            SetLeaderConfig(_configuration.ReloadConfigs(_leaderConfig.Settings));
        }

        return _leaderConfig;
    } 

    public void ReloadLeaderConfig() => SetLeaderConfig(_configuration.ReloadConfigs(_leaderConfig.Settings));
    
    private void SetLeaderConfig(IConfiguration config)
    {
        _leaderConfig = config.GetSection("leader").Get<LeaderConfig>();
        if (IsLeader)
        {
            _leaderConfig.Connectors.Clear();
            foreach (var section in config.GetSection("leader:connectors").GetChildren())
            {
                _leaderConfig.Connectors.Add(section.Key, section.ToJson());
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
        => GetBatchConfig(connector).Retries?.Errors ?? new ErrorsConfig { Tolerance = ErrorTolerance.All };

    public RetryConfig GetRetriesConfig(string connector)
        => GetBatchConfig(connector).Retries ?? new RetryConfig();

    public EofConfig GetEofSignalConfig(string connector)
        => GetConnectorConfig(connector)?.Batches?.EofSignal
           ?? _workerConfig.Batches?.EofSignal
           ?? new EofConfig();
    

    public BatchConfig GetBatchConfig(string connector)
    {
        NodeConfig nodeConfig = IsLeader ? _leaderConfig : _workerConfig;
        if (!(nodeConfig?.EnablePartitionEof ?? false))
        {
            return new BatchConfig { Size = 1, Parallelism = 1 };
        }

        return GetConnectorConfig(connector)?.Batches
               ?? nodeConfig.Batches
               ?? new BatchConfig();
    }

    public string GetGroupId(string connector)
    {
        return GetConnectorConfig(connector).GroupId;
    }

    public string GetLogEnhancer(string connector)
    {
        return GetConnectorConfig(connector).Log?.Provider;
    }

    public InternalTopicConfig GetTopics() => IsLeader ? _leaderConfig.Topics : _workerConfig.Topics;

    public ConverterConfig GetMessageConverters(string connector, string topic)
    {
        var shared = IsLeader ? _leaderConfig.Converters : _workerConfig.Converters;
        var converters = GetConnectorConfig(connector).Converters;
        return FindConverterConfig(converters?.Overrides?.SingleOrDefault(t => t.Topic == topic), converters,
            shared?.Overrides?.SingleOrDefault(t => t.Topic == topic), shared, DefaultConverter);
    }

    private static ConverterConfig FindConverterConfig(
        ConverterOverrideConfig localOverride,
        ConverterConfig localConfig,
        ConverterOverrideConfig globalOverride,
        ConverterConfig globalConfig,
        string defaultValue)
        =>
            new()
            {
                Key = localOverride?.Key ??
                      localConfig?.Key ?? globalOverride?.Key ?? globalConfig?.Key ?? defaultValue,
                Value = localOverride?.Value ??
                        localConfig?.Value ?? globalOverride?.Value ?? globalConfig?.Value ?? defaultValue,
                Subject = localOverride?.Subject ?? localConfig?.Subject ??
                    globalOverride?.Subject ?? globalConfig?.Subject ?? "Topic",
                Record = localOverride?.Record ??
                         localConfig?.Record ?? globalOverride?.Record ?? globalConfig?.Record,
            };

    public IList<string> GetTopics(string connector)
    {
        return IsLeader ? new List<string> { _leaderConfig.Topics.Config } : GetConnectorConfig(connector).Topics;
    }

    public IList<ProcessorConfig> GetMessageProcessors(string connector, string topic)
    {
        return GetConnectorConfig(connector).Processors?.Values.Where(p=> p.Topics == null || p.Topics.Contains(topic)).ToList() ?? new List<ProcessorConfig>();
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
        var connectors = _configuration.GetSection("worker:connectors").Get<IDictionary<string, ConnectorConfig<T>>>();
        var config = connectors?.SingleOrDefault(c => (c.Value.Name ?? c.Key) == connector).Value
            ?.Processors?.SingleOrDefault(p => p.Value != null && p.Value.Name == processor).Value;
        return config != null ? config.Settings : default;
    }

    public T GetPluginConfig<T>(string connector)
    {
        var connectors = _configuration.GetSection("worker:connectors").Get<IDictionary<string, ConnectorPluginConfig<T>>>();
        if (connectors.TryGetValue(connector, out var config) && config?.Plugin != null)
        {
            return config.Plugin.Properties;
        }
        return default;
    }

    public  T GetLogAttributes<T>(string connector)
    {
        var connectors = _configuration.GetSection("worker:connectors").Get<IDictionary<string, ConnectorLogConfig<T>>>();
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
            TimeoutMs = retries.TimeoutInMs,
            ErrorTolerated = IsErrorTolerated(connector),
            ErrorTolerance = (
                All: errorsConfig.Tolerance == ErrorTolerance.All,
                Data: errorsConfig.Tolerance == ErrorTolerance.Data,
                None: errorsConfig.Tolerance == ErrorTolerance.None)
        };
    }

    public void Validate()  
    {
        if (string.IsNullOrWhiteSpace(_workerConfig.BootstrapServers))
        {
            throw new ArgumentException("Bootstrap Servers isn't configured for worker.");
        }
        if (!(_workerConfig.Connectors?.Any() ?? false))
        {
            throw new ArgumentException("At least one connector is required for the worker to start.");
        }
            
        var hash = new HashSet<string>();
        hash.Clear();
        if (!_workerConfig.Connectors.Values.All(c => c!= null && hash.Add(c.Name) && !string.IsNullOrEmpty(c.Name)))
        {
            throw new ArgumentException("Connector Name configuration property must be specified and must be unique.");
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
