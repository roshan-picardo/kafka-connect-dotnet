using System.Text.Json.Nodes;

namespace Kafka.Connect.MongoDb.Models
{
    public class SinkConfig : PluginConfig
    {
        public string Collection { get; set; }
        public bool IsWriteOrdered { get; set; } = true;
        public JsonNode Condition { get; set; }
    }
}