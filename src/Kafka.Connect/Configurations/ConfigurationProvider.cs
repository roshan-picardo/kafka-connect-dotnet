using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Kafka.Connect.Configurations
{
    public class ConfigurationProvider : IConfigurationProvider
    {
        private readonly ILogger<IConfigurationProvider> _logger;
        private readonly WorkerConfig _workerConfig;
        public ConfigurationProvider(IOptions<WorkerConfig> options, ILogger<IConfigurationProvider> logger)
        {
            _logger = logger;
            _workerConfig = Build(options);
        }

        public FailOverConfig GetFailOverConfig()
        {
            return _workerConfig.HealthWatch?.FailOver ?? new FailOverConfig();
        }
        
        public HealthCheckConfig GetHealthCheckConfig()
        {
            return _workerConfig.HealthWatch?.HealthCheck ?? new HealthCheckConfig();
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

        public ConnectorConfig GetConnectorConfig(string connector)
        {
            return _workerConfig.Connectors?.SingleOrDefault(c => c.Name == connector) ?? throw new ArgumentException($"{connector} isn't configured.");
        }

        public IList<ConnectorConfig> GetConnectorConfigs(bool includeDisabled = false)
        {
            _workerConfig.Connectors ??= new List<ConnectorConfig>();
            return _workerConfig.Connectors.Where(c => includeDisabled || !c.Disabled).ToList();
        }

        public string GetWorkerName()
        {
            return _workerConfig.Name;
        }

        public RestartsConfig GetRestartsConfig()
        {
            return _workerConfig.HealthWatch?.Restarts ?? new RestartsConfig();
        }

        public ErrorsConfig GetErrorsConfig(string connector)
        {
            return GetConnectorConfig(connector)?.Errors 
                   ?? _workerConfig.Shared?.Errors 
                   ?? new ErrorsConfig {Tolerance = ErrorTolerance.All};
        }
        
        public RetryConfig GetRetriesConfig(string connector)
        {
            return GetConnectorConfig(connector)?.Retries 
                   ?? _workerConfig.Shared?.Retries 
                   ?? new RetryConfig();
        }

        public EofConfig GetEofSignalConfig(string connector)
        {
            return GetConnectorConfig(connector)?.EofSignal
                   ?? _workerConfig.Shared?.EofSignal
                   ?? new EofConfig();
        }

        public BatchConfig GetBatchConfig(string connector)
        {
            if (_workerConfig.EnablePartitionEof ?? false)
            {
                return new BatchConfig {Size = 1, Parallelism = 1};
            }

            return GetConnectorConfig(connector)?.Batch
                   ?? _workerConfig.Shared?.Batch
                   ?? new BatchConfig();
        }

        public string GetGroupId(string connector)
        {
            return GetConnectorConfig(connector).GroupId;
        }

        public (string keyConverter, string valueConverter) GetMessageConverters(string connector, string topic)
        {
            var shared = _workerConfig.Shared?.Deserializers;
            var deserializers = GetConnectorConfig(connector).Deserializers;
            var keyConverter = deserializers?.Overrides?.SingleOrDefault(t => t.Topic == topic)?.Key 
                               ?? deserializers?.Key
                               ?? shared?.Overrides?.SingleOrDefault(t => t.Topic == topic)?.Key 
                               ?? shared?.Key;
            var valueConverter = deserializers?.Overrides?.SingleOrDefault(t => t.Topic == topic)?.Value 
                                 ?? deserializers?.Value
                                 ?? shared?.Overrides?.SingleOrDefault(t => t.Topic == topic)?.Value 
                                 ?? shared?.Value;
            return (keyConverter, valueConverter);
        }

        public IList<string> GetTopics(string connector)
        {
            return GetConnectorConfig(connector).Topics;
        }

        public IList<ProcessorConfig> GetMessageProcessors(string connector, string topic)
        {
            return GetConnectorConfig(connector).Processors?.Where(p=> p.Topics == null || p.Topics.Contains(topic)).ToList();
        }

        public SinkConfig GetSinkConfig(string connector)
        {
            var sinkConfig = GetConnectorConfig(connector).Sink ?? new SinkConfig();
            sinkConfig.Plugin ??= GetConnectorConfig(connector).Plugin;
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

        public (bool enableAutoCommit, bool enableAutoOffsetStore) GetAutoCommitConfig()
        {
            return (_workerConfig.EnableAutoCommit ?? false, _workerConfig.EnableAutoOffsetStore ?? false);
        }

        private static WorkerConfig Build(IOptions<WorkerConfig> options)
        {
            var workerConfig = options?.Value ?? throw new ArgumentNullException(nameof(options));
            //1. Kafka 
            
            
            return workerConfig;
        }
        
    }
}