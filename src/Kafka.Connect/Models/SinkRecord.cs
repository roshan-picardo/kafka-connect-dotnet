using System;
using Confluent.Kafka;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Models;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Models;

public class SinkRecord : Plugin.Models.ConnectRecord
{
    private readonly ConsumeResult<byte[], byte[]> _consumed;

    public SinkRecord(ConsumeResult<byte[], byte[]> consumed) :
        base(consumed.Topic, consumed.Partition, consumed.Offset)
    {
        _consumed = consumed;
        StartTiming(_consumed.Message.Timestamp.UnixTimestampMs);
    }

    public Message<byte[], byte[]> GetDeadLetterMessage(Exception ex)
    {
        _consumed.Message.Headers ??= new Headers();
        _consumed.Message.Headers.Add("_errorContext",
            ByteConvert.Serialize(new DeadLetterErrorContext(_consumed.Topic, _consumed.Partition, _consumed.Offset,
                ex)));
        return _consumed.Message;
    }

    public Message<byte[], byte[]> GetConsumedMessage() => _consumed?.Message;

    public void Parsed(JToken key, JToken value)
    {
        Deserialized = new ConnectMessage<JToken, JToken>
        {
            Key = key,
            Value = value
        };
    }
}