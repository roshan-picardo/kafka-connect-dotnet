namespace Kafka.Connect.Plugin.Models;

public class ConnectorConfig
{
    public string Name { get; set; }
    public int MaxTasks { get; set; }
}