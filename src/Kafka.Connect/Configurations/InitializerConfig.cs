namespace Kafka.Connect.Configurations
{
    public class InitializerConfig
    {
        public string Prefix { get; set; }
        public string Assembly { get; init; }
        public string Class { get; init; }
    }
}