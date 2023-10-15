using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Handlers;

public interface ISinkConsumer
{
    IConsumer<byte[], byte[]> Subscribe(string connector, int taskId);

    Task<ConnectRecordBatch> Consume(IConsumer<byte[], byte[]> consumer, string connector, int taskId);
}