using Kafka.Connect.Configurations;

namespace Kafka.Connect.Models;

public class CommandContext
{
    public string Connector { get; set; }
    public string Topic { get; set; }
    public int Partition { get; set; }
    public long Offset { get; set; }
    public long Timestamp { get; set; } 
    public CommandConfig Command { get; set; }
}
