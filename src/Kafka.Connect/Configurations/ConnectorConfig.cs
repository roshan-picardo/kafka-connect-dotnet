using System.Collections.Generic;
using Kafka.Connect.Providers;

namespace Kafka.Connect.Configurations;

public class ConnectorConfig
{
    private readonly string _groupId;
    private readonly string _clientId;
    private readonly LogConfig _log;

    public ConverterConfig Converters { get; init; }
    
    public string[] Topics { get; init; }

    public Dictionary<string, TopicConfig> Overrides
    {
        get;
        init;
    }

    public string Name { get; set; }

    public string GroupId
    {
        get => _groupId ?? Name;
        init => _groupId = value;
    }
    public bool Disabled { get; init; }

    public int Tasks { get; init; }
    public bool Paused { get; init; }
    public PluginConfig Plugin { get; init; }

    public LogConfig Log
    {
        get => _log ?? new LogConfig { Provider = typeof(DefaultLogRecord).FullName };
        init => _log = value;
    }

    public FaultToleranceConfig FaultTolerance { get; init; }
        
    public IDictionary<int, ProcessorConfig> Processors { get; init; }

    public string ClientId
    {
        get => _clientId ?? Name;
        init => _clientId = value;
    }
}
