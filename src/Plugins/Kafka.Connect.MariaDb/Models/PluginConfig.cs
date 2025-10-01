using System.Text.Json;
using System.Text.Json.Nodes;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.MariaDb.Models;

public class PluginConfig
{
    public string Database { get; set; }
    public string Host { get; set; } = "localhost";
    public int Port { get; set; } = 3306;
    public string UserId { get; set; }
    public string Password { get; set; }
    
    public ChangelogConfig Changelog { get; set; }
    public IDictionary<string, CommandConfig> Commands { get; set; }
    
    public string Schema { get; set; }
    public string Table { get; set; }
    
    public string Lookup { get; set; }
    public string Filter { get; set; }
    
    public string ConnectionString => $"Server={Host};Port={Port};Database={Database};User ID={UserId};Password={Password};";
}

public class CommandConfig : Command
{
    public string Table { get; set; }
    public string Schema { get; set; }
    public string[] Keys { get; set; }

    public Dictionary<string, object> Filters { get; set; }
    
    public SnapshotConfig Snapshot { get; set; } = new();
    public override JsonNode ToJson() => JsonSerializer.SerializeToNode(this);
    public bool IsSnapshot() => Snapshot.Enabled;
    public bool IsInitial() => Snapshot.Enabled && Snapshot.Total == 0;
}

public class ChangelogConfig
{
    public string Schema { get; set; }
    public string Table { get; set; }
    public int Retention { get; set; } = 1;
}

public class SnapshotConfig
{
    public bool Enabled { get; set; }
    public string Key { get; set; }
    public long Id { get; set; }
    public long Total { get; set; }
    public long Timestamp { get; set; }
}
