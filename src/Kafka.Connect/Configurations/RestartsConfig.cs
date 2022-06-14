namespace Kafka.Connect.Configurations
{
    public class RestartsConfig
    {
        private readonly int _periodicDelayMs = 2000;
        private readonly int _retryWaitTimeMs = 30000;
        public RestartsLevel EnabledFor { get; init; }
        public int Attempts { get; init; }

        public int PeriodicDelayMs
        {
            get => _periodicDelayMs <= 0 ? 2000 : _periodicDelayMs;
            init => _periodicDelayMs = value;
        }
        public int RetryWaitTimeMs
        {
            get => _retryWaitTimeMs <= 0 ? 30000 : _retryWaitTimeMs;
            init => _retryWaitTimeMs = value;
        }
        public bool StopOnFailure { get; init; }
    }
}