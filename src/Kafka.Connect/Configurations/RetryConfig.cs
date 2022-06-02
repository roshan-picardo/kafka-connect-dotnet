namespace Kafka.Connect.Configurations
{
    public class RetryConfig
    {
        private readonly int _attempts = 3;
        private readonly int _delayTimeoutMs = 1000;

        public int Attempts
        {
            get => _attempts < 0 ? 3 : _attempts;
            init => _attempts = value;
        }

        public int DelayTimeoutMs
        {
            get => _delayTimeoutMs <= 0 ? 1000 : _delayTimeoutMs;
            init => _delayTimeoutMs = value;
        }
    }
}

    