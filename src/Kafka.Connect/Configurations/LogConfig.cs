using Kafka.Connect.Providers;

namespace Kafka.Connect.Configurations;

public class LogConfig
{
    private string _provider = null;

    public string Provider
    {
        get => string.IsNullOrWhiteSpace(_provider) ? typeof(DefaultLogRecord).FullName : _provider;
        set => _provider = value;
    }
}