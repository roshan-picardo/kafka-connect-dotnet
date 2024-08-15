using System;
using System.Linq;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Utilities;

namespace Kafka.Connect.Models;

public class SinkRecord : ConnectRecord
{
    public SinkRecord(ConsumeResult<byte[], byte[]> consumed) :
        base(consumed.Topic, consumed.Partition, consumed.Offset)
    {
        StartTiming(consumed.Message?.Timestamp.UnixTimestampMs);
        if (consumed.Message != null)
        {
            Serialized = new ConnectMessage<byte[]>
            {
                Key = consumed.Message.Key,
                Value = consumed.Message.Value,
                Headers = consumed.Message.Headers?.ToDictionary(h => h.Key, h => h.GetValueBytes())
            };
        }
        IsPartitionEof = consumed.IsPartitionEOF;
        Status = Status.Consumed;
    }
    
    public bool IsPartitionEof { get; set; }

    public Message<byte[], byte[]> GetDeadLetterMessage(Exception ex)
    {
        var deadMessage = new Message<byte[], byte[]> { Headers = new Headers() };
        if (Serialized != null)
        {
            deadMessage.Key = Serialized.Key;
            deadMessage.Value = Serialized.Value;
            deadMessage.Headers = Serialized.Headers.ToMessageHeaders();
        }
        deadMessage.Headers.Add("_errorContext", ByteConvert.Serialize(new DeadLetterErrorContext(Topic, Partition, Offset, ex)));
        return deadMessage;
    }
}
