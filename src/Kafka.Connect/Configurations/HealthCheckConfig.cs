namespace Kafka.Connect.Configurations
{
    public class HealthCheckConfig
    {
        private readonly int _initialDelayMs = 60000;
        private readonly int _periodicDelayMs = 60000;
        public bool Disabled { get; set; }
        public int InitialDelayMs 
        {
            get => _initialDelayMs <= 0 ? 60000 : _initialDelayMs;
            init => _initialDelayMs = value;
        }
        
        public int PeriodicDelayMs 
        {
            get => _periodicDelayMs <= 0 ? 60000 : +_periodicDelayMs;
            init => _periodicDelayMs = value;
        }
    }
}