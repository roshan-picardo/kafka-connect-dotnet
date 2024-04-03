namespace Kafka.Connect.Configurations;

public class CommandConfig
{
    public string Topic { get; set; }
    public string Table { get; set; }
    public Columns Columns { get; set; }
    
    public long Timestamp { get; set; } = 0;
    public object UniqueId { get; set; } = 0;
}

public class Columns
{
    public string Timestamp { get; set; }
    public string UniqueId { get; set; }
}
