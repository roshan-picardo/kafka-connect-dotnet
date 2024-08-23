using System;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Models
{
    [Serializable]
    public class DeadLetterErrorContext(ConnectRecord record)
    {
        public string Topic { get; } = record.Topic;
        public int Partition { get; } = record.Partition;
        public long Offset { get; } = record.Offset;
        public Exception Exception { get; } = record.Exception;
        public long Timestamp { get; } = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }
}