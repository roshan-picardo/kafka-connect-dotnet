using System;
using Confluent.Kafka;

namespace Kafka.Connect.Models
{
    [Serializable]
    public class MessageContext 
    {
        public MessageContext(TopicPartitionOffset topicPartitionOffset)
        {
            Topic = topicPartitionOffset.Topic;
            Partition = topicPartitionOffset.Partition;
            Offset = topicPartitionOffset.Offset;
        }
        
        public MessageContext(string topic, int partition, long offset)
        {
            Topic = topic;
            Partition = partition;
            Offset = offset;
        }
        public string Topic { get; }
        public int Partition { get; }
        public long Offset { get; }
    }
}