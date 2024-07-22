using System.Text.Json;
using System.Text.Json.Nodes;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Postgres.Models;

public class PluginConfig
{
    public string Database { get; set; }
    public string Host { get; set; }
    public int Port { get; set; } = 5432;
    public string UserId { get; set; }
    public string Password { get; set; }
    
    public ChangelogConfig Changelog { get; set; }
    public IDictionary<string, CommandConfig> Commands { get; set; }
    
    public string Schema { get; set; } = "public";
    public string Table { get; set; }
    
    public string Lookup { get; set; }
    public string Filter { get; set; }
    
    public string ConnectionString => $"Host={Host};Port={Port};User Id={UserId};Password='{Password}';Database={Database}";
}

public class CommandConfig : Command
{
    public string Table { get; set; }
    public string Schema { get; set; } = "public";
    public string[] Keys { get; set; }

    public Dictionary<string, object> Filters { get; set; }
    
    public SnapshotConfig Snapshot { get; set; } = new();
    public override JsonNode ToJson() => JsonSerializer.SerializeToNode(this);
    public bool IsSnapshot() => Snapshot.Enabled;
    public bool IsInitial() => Snapshot.Enabled && Snapshot.Total == 0;
}

public class ChangelogConfig
{
    public bool Enabled => string.IsNullOrWhiteSpace(Table);
    public string Schema { get; set; } = "public";
    public string Table { get; set; }
    public int RetentionInDays { get; set; } = 7;
}

public class SnapshotConfig
{
    public bool Enabled { get; set; }
    public string Key { get; set; }
    public long Id { get; set; }
    public long Total { get; set; }
    public long Timestamp { get; set; }
}