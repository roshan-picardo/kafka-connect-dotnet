using System.Collections.Generic;
using System.Linq;

namespace Kafka.Connect.Configurations;

public class WorkerConfig : NodeConfig
{
    private readonly IDictionary<string, ConnectorConfig> _connectors;

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

                if (connector.Type == ConnectorType.Source)
                {
                    connector.Topics.Clear();
                    connector.Topics.Add(Topics.Command);
                }

            }
            return _connectors;
        }
        init => _connectors = value;
    }
}