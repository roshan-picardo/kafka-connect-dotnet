using System;
using Confluent.Kafka;

namespace Kafka.Connect.Models
{
    [Serializable]
    public class DeadLetterErrorContext
    {
        public string Topic { get; }
        public int Partition { get; }
        public long Offset { get; }
        public Exception Exception { get; }

        public DeadLetterErrorContext(string topic, int partition, long offset, Exception exception)
        {
            Topic = topic;
            Partition = partition;
            Offset = offset;
            Exception = exception;
        }
    }
}