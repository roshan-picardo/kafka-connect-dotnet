namespace Kafka.Connect.Config.Models
{
    public class DeadLetterConfig
    {
        public string Topic { get; init; }
        public bool? Create { get; init; }
        public int? Partitions { get; set; }
        public short? Replication { get; set; }
        public bool InUse => !string.IsNullOrWhiteSpace(Topic);
    }
}