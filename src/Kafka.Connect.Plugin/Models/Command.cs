using System.Text.Json.Nodes;

namespace Kafka.Connect.Plugin.Models;

public abstract class Command
{
    public string Topic { get; set; }

    public int Version { get; set; }

    public abstract JsonNode ToJson();
}
