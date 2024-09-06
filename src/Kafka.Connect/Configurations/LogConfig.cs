using Kafka.Connect.Providers;

namespace Kafka.Connect.Configurations;

public class LogConfig
{
    private string _provider;

    public string Provider
    {
        get => string.IsNullOrWhiteSpace(_provider) ? typeof(DefaultLogRecord).FullName : _provider;
        set => _provider = value;
    }
}

public class LogConfig<T> : LogConfig
{
    public T Attributes { get; set; }
}

public class ConnectorLogConfig<T> : ConnectorConfig
{
    public new LogConfig<T> Log { get; set; }
}