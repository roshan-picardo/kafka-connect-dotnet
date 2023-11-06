using System.Collections.Generic;

namespace Kafka.Connect.Plugin.Models;

public class ConnectMessage<T>
{
    public T Key { get; set; }

    public T Value { get; set; }

    public IDictionary<string, T> Headers { get; set; }
}