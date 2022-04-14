namespace Kafka.Connect.Config.Models
{
    public class HealthCheckConfig
    {
        public bool Enabled { get; set; }
        public int InitialDelaySeconds { get; set; }
        public int PeriodSeconds { get; set; }
    }
}