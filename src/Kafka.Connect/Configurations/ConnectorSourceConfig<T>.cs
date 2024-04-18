namespace Kafka.Connect.Configurations;

public class ConnectorSourceConfig<T> : ConnectorConfig
{
    public new SourceConfig<T> Source { get; set; }
}