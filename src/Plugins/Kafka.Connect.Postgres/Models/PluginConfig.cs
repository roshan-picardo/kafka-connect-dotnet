namespace Kafka.Connect.Postgres.Models;

public class PluginConfig
{
    public string Database { get; set; }
    public string Host { get; set; }
    public int Port { get; set; } = 5432;
    public string UserId { get; set; }
    public string Password { get; set; }
    
    public string ConnectionString => $"Host={Host};Port={Port};User Id={UserId};Password='{Password}';Database={Database}";
}