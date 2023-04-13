using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;

namespace Kafka.Connect.Configurations
{
    public class WorkerConfig : ConsumerConfig
    {
        private readonly string _name;
        private readonly IDictionary<string, ConnectorConfig> _connectors;

        public string Name
        {
            get => _name ?? Environment.GetEnvironmentVariable("WORKER_HOST") ?? Environment.MachineName;
            init => _name = value;
        }

        public IDictionary<string, ConnectorConfig> Connectors
        {
            get
            {
                if (_connectors == null || !_connectors.Any())
                {
                    return _connectors;
                }
                foreach (var (name, connector) in _connectors)
                {
                    if (connector != null && string.IsNullOrEmpty(connector.Name))
                    {
                        connector.Name = name;
                    }
                }
                return _connectors;
            }
            init => _connectors = value;
        }

        public PluginConfig Plugins { get; init; }
        public HealthCheckConfig HealthCheck { get; init; }
        public FailOverConfig FailOver { get; init; }
        public RestartsConfig Restarts { get; init; }
        public RetryConfig Retries { get; init; }
        public BatchConfig Batches { get; set; }
    }
}