using System;

namespace Kafka.Connect.Models
{
    [Serializable]
    public class EndOfPartitionMessage
    {
        public string Connector { get; set; }
        public int TaskId { get; set; }
        public string Topic { get; set; }
        public int Partition { get; set; }
        public long Offset { get; set; }
    }
}