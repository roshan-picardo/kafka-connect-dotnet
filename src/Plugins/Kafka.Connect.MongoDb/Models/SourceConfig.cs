using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.MongoDb.Models;

public class SourceConfig : PluginConfig
{
    public IDictionary<string, CommandConfig> Commands { get; set; }
}

public class CommandConfig : Command
{
    public long Timestamp { get; set; }
    public IDictionary<string, object> Keys { get; set; }
    public string Collection { get; set; }
    public string TimestampColumn { get; set; }
    public string[] KeyColumns { get; set; }
        
    public override JsonNode ToJson() => JsonSerializer.SerializeToNode(this);
}