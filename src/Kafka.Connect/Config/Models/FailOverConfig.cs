namespace Kafka.Connect.Config.Models
{
    public class FailOverConfig
    {
        public bool Enabled { get; set; }
        public int TimeoutMs { get; set; }
        public int Threshold { get; set; }
        public int DelayMs { get; set; }
    }
}