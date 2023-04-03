namespace Kafka.Connect.Configurations;

public class LogConfig<T> : LogConfig
{
    public T Attributes { get; set; }
}