namespace Kafka.Connect.Plugin.Models;

public class ConnectMessage<TKey, TValue>
{
    public TKey Key { get; set; }

    public TValue Value { get; set; }
}