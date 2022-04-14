namespace Kafka.Connect.Configurations
{
    public class EofConfig
    {
        public bool Enabled { get; init; }
        public string Topic { get; init; }
    }
}