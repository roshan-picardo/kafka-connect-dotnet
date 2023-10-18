using System;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Extensions;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Models
{
    public class ConnectRecord : Plugin.Models.ConnectRecord
    {
        private readonly ConsumeResult<byte[], byte[]> _consumed;

        public ConnectRecord(ConsumeResult<byte[], byte[]> consumed) :
            base(consumed.Topic, consumed.Partition, consumed.Offset)
        {
            _consumed = consumed;
            StartTiming(_consumed.Message.Timestamp.UnixTimestampMs);
        }

        public ConnectRecord(string topic, JToken key, JToken value) : base(topic, -1, -1)
        {
            Parsed(key, value);
            StartTiming();
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
    }
}