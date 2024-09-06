namespace Kafka.Connect.Configurations;

public class PluginConfig
{
    public string Name { get; init; }
    public ConnectorType Type { get; init; }
    public string Handler { get; init; }
    public StrategyConfig Strategy { get; init; }
}

public enum ConnectorType
{
    Leader,
    Worker,
    Sink,
    Source
}

public class PluginConfig<T> : PluginConfig
{
    public T Properties { get; init; }
}

public class ConnectorPluginConfig<T> : ConnectorConfig
{
    public new PluginConfig<T> Plugin { get; init; }
}
