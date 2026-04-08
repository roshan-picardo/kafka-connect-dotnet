using System.Collections.Generic;
using System.Linq;

namespace Kafka.Connect.Configurations;

public class WorkerConfig : NodeConfig
{
    private readonly IDictionary<string, ConnectorConfig> _connectors = new Dictionary<string, ConnectorConfig>();

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
                if (connector == null) continue;
                
                if (string.IsNullOrEmpty(connector.Name))
                {
                    connector.Name = name;
                }
            }
            return _connectors;
        }
        init => _connectors = value ?? new  Dictionary<string, ConnectorConfig>();
    }
    
    public ConnectorConfig Connector => new()
    {
        Name = Name,
        Topics = Topics.TryGetValue(TopicType.Config, out var value) ? [value] : null,
        Tasks = 1,
        GroupId = GroupId,
        Plugin = new PluginConfig
        {
            Type = ConnectorType.Worker
        }
    };

    public bool Standalone { get; set; } = true;
    
    public string Settings { get; init; }
    
    public IDictionary<int, ProcessorConfig> Processors { get; init; }
}