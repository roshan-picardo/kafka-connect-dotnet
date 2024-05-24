using System.Text.Json;
using System.Text.Json.Nodes;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Postgres.Models;

public class PostgresSourceConfig
{
    public string Database { get; set; }
    public string Host { get; set; }
    public int Port { get; set; } = 5432;
    public string UserId { get; set; }
    public string Password { get; set; }
    
    public string ConnectionString => $"Host={Host};Port={Port};User Id={UserId};Password='{Password}';Database={Database}";
    
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