using System.Text.Json.Nodes;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Models;

public class SourceRecord : ConnectRecord
{
    public SourceRecord(string topic, JsonNode key, JsonNode value) : base(topic, -1, -1)
    {
        Deserialized = new ConnectMessage<JsonNode>
        {
            Key = key,
            Value = value
        };
        StartTiming();
    }
    public int Timestamp { get; set; }
    public object UniqueId { get; set; }
}