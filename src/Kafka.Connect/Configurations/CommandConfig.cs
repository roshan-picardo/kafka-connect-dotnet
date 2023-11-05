namespace Kafka.Connect.Configurations;

public class CommandConfig
{
    public string Topic { get; set; }
    public string Text { get; set; }
    private CommandType Type { get; set; }
    public string Timestamp { get; set; }
}