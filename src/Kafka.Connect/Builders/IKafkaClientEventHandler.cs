using System.Collections.Generic;
using Confluent.Kafka;

namespace Kafka.Connect.Builders
{
    public interface IKafkaClientEventHandler
    {
        void HandleError(Error error);
        void HandleLogMessage(LogMessage log);
        void HandleStatistics(string stats);
        void HandlePartitionAssigned(string connector, int taskId, IList<TopicPartition> partitions);
        void HandlePartitionRevoked(string connector, int taskId, IList<TopicPartitionOffset> offsets);
        void HandleOffsetCommitted(CommittedOffsets offsets);
    }
}