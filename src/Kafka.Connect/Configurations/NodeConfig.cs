using System;
using System.Collections.Generic;
using Confluent.Kafka;

namespace Kafka.Connect.Configurations;

public class NodeConfig : ConsumerConfig
{
    private readonly string _name;

    public string Name
    {
        get => _name ?? Environment.GetEnvironmentVariable("NODE_NAME") ?? Environment.MachineName;
        init => _name = value;
    }
        
    public IDictionary<string, TopicConfig> Topics { get; init; }
    public PluginAssemblyConfig Plugins { get; init; }
    public HealthCheckConfig HealthCheck { get; init; }
    public FailOverConfig FailOver { get; init; }
    public RestartsConfig Restarts { get; init; }
    public ConverterConfig Converters { get; init; }
    public FaultToleranceConfig FaultTolerance { get; init; }
}