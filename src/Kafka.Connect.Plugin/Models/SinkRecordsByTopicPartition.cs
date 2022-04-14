using System.Collections.Generic;

namespace Kafka.Connect.Plugin.Models
{
    public class SinkRecordsByTopicPartition
    {
        public string Topic { get; init; }
        public int Partition { get; init; }
        public IList<SinkRecord> Batch { get; init; }
    }
}