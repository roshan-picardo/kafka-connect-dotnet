namespace Kafka.Connect.Config.Models
{
    public class RetryConfig
    {
        public int Attempts { get; set; }
        public int DelayTimeoutMs { get; set; }
        public bool Continue { get; set; }
    }
}

    