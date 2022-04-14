namespace Kafka.Connect.Config.Models
{
    public class SelfHealingConfig
    {
        public int Attempts { get; set; }
        public int DelayTimeoutMs { get; set; }
    }
}