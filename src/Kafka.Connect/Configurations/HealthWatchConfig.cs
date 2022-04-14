namespace Kafka.Connect.Configurations
{
    public class HealthWatchConfig
    {
        public FailOverConfig FailOver { get; set; }
        public HealthCheckConfig HealthCheck { get; set; }
        public RestartsConfig Restarts { get; set; }
       
    }
}