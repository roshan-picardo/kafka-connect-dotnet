using System.Text.Json;
using System.Text.Json.Nodes;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Postgres.Models;

public class SourceConfig : PluginConfig
{
    public IDictionary<string, CommandConfig> Commands { get; set; }
}

public class CommandConfig : Command
{
    public string Table { get; set; }
    public string Schema { get; set; } = "public";
    public string TimestampColumn { get; set; }
    public string[] KeyColumns { get; set; }
    public long Timestamp { get; set; }
    public IDictionary<string, object> Keys { get; set; }

    public override JsonNode ToJson() => JsonSerializer.SerializeToNode(this);
}