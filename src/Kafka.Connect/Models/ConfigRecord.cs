using System.Text.Json.Nodes;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Models;

public class ConfigRecord : ConnectRecord
{
    public ConfigRecord(string topic,
        JsonNode key,
        JsonNode value) : base(topic, -1, -1)
    {
        Deserialized = new ConnectMessage<JsonNode>
        {
            Key = key,
            Value = value,
        };
    }
    
    public bool UpdateLocal { get; set; }
}