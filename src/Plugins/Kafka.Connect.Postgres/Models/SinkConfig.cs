namespace Kafka.Connect.Postgres.Models;

public class SinkConfig : PluginConfig
{
    public string Schema { get; set; } = "public";
    public string Table { get; set; }
    public FilterConfig Filter { get; set; }
}

public class FilterConfig
{
    public string Condition { get; set; } 
    public string[] Keys { get; set; }
}