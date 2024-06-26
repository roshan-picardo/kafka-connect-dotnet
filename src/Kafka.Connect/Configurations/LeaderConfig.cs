using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;

namespace Kafka.Connect.Configurations;

public class LeaderConfig : NodeConfig
{
    private readonly IDictionary<string, JsonNode> _connectors = new Dictionary<string, JsonNode>();
    public IDictionary<string, JsonNode> Connectors
    {
        get
        {
            if (_connectors == null || !_connectors.Any())
            {
                return _connectors;
            }
            foreach (var (name, connector) in _connectors)
            {
                if (connector != null && string.IsNullOrEmpty(connector["name"]?.GetValue<string>()))
                {
                    connector["name"] = name;
                }
            }
            return _connectors;
        }
        init => _connectors = value;
    }

    public ConnectorConfig Connector => new()
    {
        Name = Name,
        Topic = Topics.Config,
        MaxTasks = 1,
        GroupId = GroupId
    };
    
    public string Settings { get; set; }
}