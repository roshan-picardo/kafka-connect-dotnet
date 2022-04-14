using System;
using System.Collections.Generic;
using Confluent.Kafka;

namespace Kafka.Connect.Configurations
{
    public class WorkerConfig : ConsumerConfig
    {
        private readonly string _name;

        public string Name
        {
            get => _name ?? Environment.GetEnvironmentVariable("WORKER_HOST") ?? Environment.MachineName;
            init => _name = value;
        }

        public IList<ConnectorConfig> Connectors { get; set; }
        public IList<PluginConfig> Plugins { get; init; }
        public HealthWatchConfig HealthWatch { get; init; }
        public SharedConfig Shared { get; init; }
    }
}