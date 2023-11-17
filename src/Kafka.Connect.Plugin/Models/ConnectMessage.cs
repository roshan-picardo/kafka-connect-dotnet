using System.Collections.Generic;

namespace Kafka.Connect.Plugin.Models;

public class ConnectMessage<T>
{
    public bool Skip { get; set; }
    public T Key { get; set; }

    public T Value { get; set; }

    public IDictionary<string, T> Headers { get; set; }
}