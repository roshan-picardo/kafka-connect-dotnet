namespace Kafka.Connect.Config.Models
{
    public class EndOfPartitionConfig
    {
        public bool? Enabled { get; init; }
        public string Topic { get; init; }
    }
}