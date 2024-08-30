namespace Kafka.Connect.Configurations
{
    public class RetryConfig
    {
        public int Attempts { get; set; }
        public int Interval { get; set; }
    }
    
    public class RetryConfigOld
    {
        private readonly int _attempts = 3;
        private readonly int _timeoutInMs = 1000;

        public int Attempts
        {
            get => _attempts < 0 ? 3 : _attempts;
            init => _attempts = value;
        }

        public int TimeoutInMs
        {
            get => _timeoutInMs <= 0 ? 1000 : _timeoutInMs;
            init => _timeoutInMs = value;
        }
        public ErrorsConfig Errors { get; init; }
    }
}

    