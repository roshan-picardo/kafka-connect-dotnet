using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Handlers
{
    public interface IPartitionHandler
    {
        void CommitOffsets(ConnectRecordBatch batch, IConsumer<byte[], byte[]> consumer);
        
        void Commit(IConsumer<byte[], byte[]> consumer, IList<(string Topic, int Partition, long Offset)> offsets);

        Task NotifyEndOfPartition(
            IConsumer<byte[], byte[]> consumer,
            string connector,
            int taskId,
            IList<(string Topic, int Partition, long Offset)> eofPartitions,
            IList<(string Topic, int Partition, long Offset)> commitReadyOffsets);

        Task NotifyEndOfPartition(ConnectRecordBatch batch, string connector, int taskId);
    }
}