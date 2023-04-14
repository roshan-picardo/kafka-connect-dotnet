using System;
using System.Linq;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Extensions;

namespace Kafka.Connect.Models;

public class ConnectSinkRecord : Plugin.Models.SinkRecord
{
    private readonly ConsumeResult<byte[], byte[]> _consumed;

    public ConnectSinkRecord(ConsumeResult<byte[], byte[]> consumed) : 
        base(consumed.Topic, consumed.Partition, consumed.Offset, consumed.Message.Key, consumed.Message.Value,
        consumed.Message.Headers.ToDictionary(h => h.Key, h => h.GetValueBytes()))
    {
        _consumed = consumed;
        Consumed = consumed;
        StartTiming(_consumed.Message.Timestamp.UnixTimestampMs);
    }
    
    public ConsumeResult<byte[], byte[]> Consumed { get; private set; }

    public Message<byte[], byte[]> GetDeadLetterMessage(Exception ex)
    {
        _consumed.Message.Headers.Add("_errorContext",
            ByteConvert.Serialize(new DeadLetterErrorContext(_consumed.Topic, _consumed.Partition, _consumed.Offset, ex)));
        return _consumed.Message;
    }
}