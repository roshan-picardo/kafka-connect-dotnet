using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Kafka.Connect.Configurations;
using Kafka.Connect.Plugin;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

namespace Kafka.Connect.Providers
{
    public class ConfigurationProvider : IConfigurationProvider, Kafka.Connect.Plugin.Providers.IConfigurationProvider
    {
        private readonly IConfiguration _configuration;
        private readonly WorkerConfig _workerConfig;
        public ConfigurationProvider(IOptions<WorkerConfig> options, IConfiguration configuration)
        {
            _configuration = configuration;
            _workerConfig = options?.Value ?? throw new ArgumentNullException(nameof(options));
        }

        public FailOverConfig GetFailOverConfig()
        {
            return _workerConfig.FailOver ?? new FailOverConfig();
        }
        
        public HealthCheckConfig GetHealthCheckConfig()
        {
            return _workerConfig.HealthCheck ?? new HealthCheckConfig();
        }
        
        public ConnectorConfig GetConnectorConfig(string connector)
        {
            return _workerConfig.Connectors?.Values.SingleOrDefault(c => c.Name == connector) ??
                   throw new ArgumentException($"{connector} isn't configured.");
        }

        public ConsumerConfig GetConsumerConfig(string connector = null)
        {
            if (connector == null)
            {
                return _workerConfig;
            }
            var config = GetConnectorConfig(connector);
            _workerConfig.GroupId = config.GroupId;
            _workerConfig.ClientId = config.ClientId;
            return _workerConfig;
        }

        public ProducerConfig GetProducerConfig(string connector = null)
        {
            var consumerConfig = GetConsumerConfig(connector);
            return new ProducerConfig(consumerConfig);
        }

        public IList<ConnectorConfig> GetAllConnectorConfigs(bool includeDisabled = false)
        {
            return _workerConfig.Connectors?.Values.Where(c => includeDisabled || !c.Disabled).ToList() ??
                   throw new ArgumentException("Connectors aren't configured.");
        }

        public string GetWorkerName()
        {
            return _workerConfig.Name;
        }

        public RestartsConfig GetRestartsConfig()
        {
            return _workerConfig.Restarts ?? new RestartsConfig();
        }

        public ErrorsConfig GetErrorsConfig(string connector)
        {
            return GetConnectorConfig(connector).Retries?.Errors 
                   ?? _workerConfig.Retries?.Errors 
                   ?? new ErrorsConfig {Tolerance = ErrorTolerance.All};
        }
        
        public RetryConfig GetRetriesConfig(string connector)
        {
            return GetConnectorConfig(connector)?.Retries 
                   ?? _workerConfig.Retries 
                   ?? new RetryConfig();
        }

        public EofConfig GetEofSignalConfig(string connector)
        {
            return GetConnectorConfig(connector)?.Batches?.EofSignal
                   ?? _workerConfig.Batches?.EofSignal
                   ?? new EofConfig();
        }

        public BatchConfig GetBatchConfig(string connector)
        {
            if (!(_workerConfig.EnablePartitionEof ?? false))
            {
                return new BatchConfig {Size = 1, Parallelism = 1};
            }

            return GetConnectorConfig(connector)?.Batches
                   ?? _workerConfig.Batches
                   ?? new BatchConfig();
        }

        public string GetGroupId(string connector)
        {
            return GetConnectorConfig(connector).GroupId;
        }

        public (string Key, string Value) GetMessageConverters(string connector, string topic)
        {
            var shared = _workerConfig.Batches?.Deserializers;
            var deserializers = GetConnectorConfig(connector).Batches?.Deserializers;
            var keyConverter = deserializers?.Overrides?.SingleOrDefault(t => t.Topic == topic)?.Key 
                               ?? deserializers?.Key
                               ?? shared?.Overrides?.SingleOrDefault(t => t.Topic == topic)?.Key 
                               ?? shared?.Key;
            var valueConverter = deserializers?.Overrides?.SingleOrDefault(t => t.Topic == topic)?.Value 
                                 ?? deserializers?.Value
                                 ?? shared?.Overrides?.SingleOrDefault(t => t.Topic == topic)?.Value 
                                 ?? shared?.Value;
            return (keyConverter ?? Constants.DefaultDeserializer, valueConverter ?? Constants.DefaultDeserializer);
        }

        public IList<string> GetTopics(string connector)
        {
            return GetConnectorConfig(connector).Topics;
        }

        public IList<ProcessorConfig> GetMessageProcessors(string connector, string topic)
        {
            return GetConnectorConfig(connector).Processors?.Values.Where(p=> p.Topics == null || p.Topics.Contains(topic)).ToList() ?? new List<ProcessorConfig>();
        }

        public SinkConfig GetSinkConfig(string connector)
        {
            var connectorConfig = GetConnectorConfig(connector);
            var sinkConfig = connectorConfig.Sink ?? new SinkConfig();
            sinkConfig.Plugin ??= connectorConfig.Plugin;
            return sinkConfig;
        }

        public bool IsErrorTolerated(string connector)
        {
            return GetErrorsConfig(connector).Tolerance == ErrorTolerance.All;
        }

        public bool IsDeadLetterEnabled(string connector)
        {
            var errors = GetErrorsConfig(connector);
            return errors.Tolerance == ErrorTolerance.All && !string.IsNullOrWhiteSpace(errors.Topic);
        }

        public (bool EnableAutoCommit, bool EnableAutoOffsetStore) GetAutoCommitConfig()
        {
            return (_workerConfig.EnableAutoCommit ?? false, _workerConfig.EnableAutoOffsetStore ?? false);
        }

        public T GetProcessorSettings<T>(string connector, string processor)
        {
            var connectors = _configuration.GetSection("worker:connectors").Get<IDictionary<string, ConnectorConfig<T>>>();
            var config = connectors?.SingleOrDefault(c => (c.Value.Name ?? c.Key) == connector).Value
                             ?.Processors?.SingleOrDefault(p => p.Value != null && p.Value.Name == processor).Value;
            return config != null ? config.Settings : default;
        }

        public T GetSinkConfigProperties<T>(string connector, string plugin = null)
        {
            var connectors = _configuration.GetSection("worker:connectors").Get<IDictionary<string, ConnectorSinkConfig<T>>>();
            var config = connectors?.SingleOrDefault(c => (c.Value.Name ?? c.Key) == connector && (!string.IsNullOrWhiteSpace(plugin) || c.Value.Plugin == plugin)).Value?.Sink;
            return config != null ? config.Properties : default;
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
                if (!_workerConfig.Plugins.Initializers.Select(p => p.Key).Contains(connector.Plugin))
                {
                    throw new ArgumentException(
                        $"Connector: {connector.Name} is not associated to any of the available Plugins: [ {string.Join(", ", _workerConfig.Plugins.Initializers.Select(p => p.Key))} ].");
                }
            }
        }
    }
}