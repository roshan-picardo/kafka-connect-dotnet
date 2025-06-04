using System.Text.Json;
using System.Text.Json.Nodes;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.SqlServer.Models;

public class PluginConfig
{
    public string Database { get; set; }
    public string Server { get; set; }
    public int Port { get; set; } = 1433;
    public string UserId { get; set; }
    public string Password { get; set; }
    public bool IntegratedSecurity { get; set; } = false;
    public bool TrustServerCertificate { get; set; } = true;
    
    public ChangelogConfig Changelog { get; set; }
    public IDictionary<string, CommandConfig> Commands { get; set; }
    
    public string Schema { get; set; } = "dbo";
    public string Table { get; set; }
    
    public string Lookup { get; set; }
    public string Filter { get; set; }
    
    public string ConnectionString =>
        IntegratedSecurity
            ? $"Server={Server},{Port};Database={Database};Integrated Security=True;TrustServerCertificate={TrustServerCertificate};"
            : $"Server={Server},{Port};Database={Database};User Id={UserId};Password={Password};TrustServerCertificate={TrustServerCertificate};";
}

public class CommandConfig : Command
{
    public string Table { get; set; }
    public string Schema { get; set; } = "dbo";
    public string[] Keys { get; set; }

    public Dictionary<string, object> Filters { get; set; }
    
    public SnapshotConfig Snapshot { get; set; } = new();
    public override JsonNode ToJson() => JsonSerializer.SerializeToNode(this);
    public bool IsSnapshot() => Snapshot.Enabled;
    public bool IsInitial() => Snapshot.Enabled && Snapshot.Total == 0;
}

public class ChangelogConfig
{
    public string Schema { get; set; } = "dbo";
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
