using System.Text.Json;
using System.Text.Json.Nodes;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Cassandra.Models;

public class PluginConfig
{
    public string[] Hosts { get; set; } = [];
    public int Port { get; set; } = 9042;
    public string Keyspace { get; set; }
    public string Table { get; set; }
    public string Lookup { get; set; }
    public string Filter { get; set; }
    public IDictionary<string, CommandConfig> Commands { get; set; } = new Dictionary<string, CommandConfig>();
}

public class CommandConfig : Command
{
    public string Table { get; set; }
    public string Keyspace { get; set; }
    public string[] Keys { get; set; } = [];
    public Dictionary<string, object> Filters { get; set; } = new Dictionary<string, object>();

    public SnapshotConfig Snapshot { get; set; } = new SnapshotConfig();

    public override JsonNode ToJson() => JsonSerializer.SerializeToNode(this);
    public bool IsSnapshot() => Snapshot.Enabled;
    public bool IsInitial() => Snapshot.Enabled && Snapshot.Total == 0;
}

public class SnapshotConfig
{
    public bool Enabled { get; set; }
    public string Key { get; set; }
    public long Id { get; set; }
    public long Total { get; set; }
    public long Timestamp { get; set; }
}
