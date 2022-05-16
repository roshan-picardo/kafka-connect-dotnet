using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Handlers
{
    public interface IPartitionHandler
    {
        void CommitOffsets(SinkRecordBatch batch, IConsumer<byte[], byte[]> consumer);

        Task NotifyEndOfPartition(SinkRecordBatch batch, string connector, int taskId);
    }
}