namespace Kafka.Connect.Configurations;

public class ConnectorSinkConfig<T> : ConnectorConfig
{
    public new SinkConfig<T> Sink { get; set; }
}