using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Models;

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

    public static SinkRecord ToSinkRecord(this ConsumeResult<byte[], byte[]> consumed)
    {
        if (consumed == null) return new SinkRecord();
        return new SinkRecord(consumed.Topic, consumed.Partition, consumed.Offset, consumed.Message.Key,
            consumed.Message.Value, consumed.Message.Headers.ToDictionary(h => h.Key, h => h.GetValueBytes()));
    }

    public static ConsumeResult<byte[], byte[]> ToConsumeResult(this SinkRecord record)
    {
        return null;
    }
}