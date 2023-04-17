using System.Collections.Generic;
using Confluent.Kafka;

namespace Kafka.Connect.Utilities;

public static class MessageExtensions
{
    public static Headers ToMessageHeaders(this IDictionary<string, byte[]> dictionary)
    {
        var headers = new Headers();
        if (dictionary == null) return headers;
        foreach (var (key, value) in dictionary)
        {
            headers.Add(key, value);
        }

        return headers;
    }
}