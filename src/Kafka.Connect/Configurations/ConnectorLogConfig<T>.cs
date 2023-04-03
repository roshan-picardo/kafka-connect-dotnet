namespace Kafka.Connect.Configurations;

public class ConnectorLogConfig<T> : ConnectorConfig
{
    public new LogConfig<T> Log { get; set; }
}