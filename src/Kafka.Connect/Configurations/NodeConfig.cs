using System;
using Confluent.Kafka;

namespace Kafka.Connect.Configurations
{
    public class NodeConfig : ConsumerConfig
    {
        private readonly string _name;

        public string Name
        {
            get => _name ?? Environment.GetEnvironmentVariable("NODE_NAME") ?? Environment.MachineName;
            init => _name = value;
        }
        
        public InternalTopicConfig Topics { get; set; }
        public PluginConfig Plugins { get; init; }
        public HealthCheckConfig HealthCheck { get; init; }
        public FailOverConfig FailOver { get; init; }
        public RestartsConfig Restarts { get; init; }
        public RetryConfig Retries { get; init; }
        public BatchConfig Batches { get; set; }
    }
}