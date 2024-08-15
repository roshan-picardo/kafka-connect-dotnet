using System.Collections.Generic;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Models;

public class SourceRecord : ConnectRecord
{
    public IDictionary<string, object> Keys =>
        Deserialized.Key[nameof(Keys)]?.ToDictionary(nameof(Keys), true)  ?? new Dictionary<string, object>();

    protected override T Clone<T>(ConnectRecord record)
        => new SourceRecord
        {
            Topic = record.Topic,
            Partition = record.Partition,
            Offset = record.Offset,
            Deserialized = record.Deserialized,
            Serialized = record.Serialized,
            Status = record.Status,
            LogTimestamp = record.LogTimestamp,
            Exception = record.Exception
        } as T;
}
