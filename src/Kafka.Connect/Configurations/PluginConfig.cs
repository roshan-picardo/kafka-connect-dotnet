namespace Kafka.Connect.Configurations;

public class PluginConfig
{
    public string Name { get; set; }
    public ConnectorType Type { get; set; }
    public string Handler { get; set; }
    public StrategyConfig Strategy { get; set; }
}

public class PluginConfig<T> : PluginConfig
{
    public T Properties { get; set; }
}

public class ConnectorPluginConfig<T> : ConnectorConfig
{
    public new PluginConfig<T> Plugin { get; set; }
}
