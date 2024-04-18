using System.Collections.Generic;
using System.Text.Json.Nodes;

namespace Kafka.Connect.Plugin.Models;

public abstract class Command
{
    public string Topic { get; set; }
    public int Version { get; set; }
    public long Timestamp { get; set; }
    public IDictionary<string, object> Keys { get; set; }

    public abstract JsonNode ToJson();
}
