using System.Text.Json;
using System.Text.Json.Nodes;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Postgres.Models;

public class SourceConfig : PluginConfig
{
    public Changelog Changelog { get; set; }
    public IDictionary<string, CommandConfig> Commands { get; set; }
}

public class CommandConfig : Command
{
    public string Table { get; set; }
    public string Schema { get; set; } = "public";
    public string[] Keys { get; set; }
    public FilterConfig Filters { get; set; }
    public IDictionary<string, object> Lookup =>
        Filters.Keys.ToDictionary(key => key, key => Filters.Values?.GetValueOrDefault(key));
    public SnapshotConfig Snapshot { get; set; } = new();
    public override JsonNode ToJson() => JsonSerializer.SerializeToNode(this);
    public bool IsSnapshot() => Snapshot.Enabled;
    public bool IsInitial() => Snapshot.Enabled && Snapshot.Total == 0;
}

public class Changelog
{
    public string Schema { get; set; } = "public";
    public string Table { get; set; }
}

public class SnapshotConfig
{
    public bool Enabled { get; set; }
    public string Key { get; set; }
    public long Id { get; set; }
    public long Total { get; set; }
    public long Timestamp { get; set; }
}
