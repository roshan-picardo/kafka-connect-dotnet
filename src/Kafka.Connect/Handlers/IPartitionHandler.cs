using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Handlers
{
    public interface IPartitionHandler
    {
        void CommitOffsets(ConnectRecordBatch batch, IConsumer<byte[], byte[]> consumer);

        Task NotifyEndOfPartition(ConnectRecordBatch batch, string connector, int taskId);
    }
}