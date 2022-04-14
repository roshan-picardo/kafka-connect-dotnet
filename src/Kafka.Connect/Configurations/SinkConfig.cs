namespace Kafka.Connect.Configurations
{
    public class SinkConfig
    {
        public string Plugin { get; set; }
        public string Handler { get; set; }
    }

    public class SinkConfig<T> : SinkConfig
    {
        public T Properties { get; set; }
    }
}