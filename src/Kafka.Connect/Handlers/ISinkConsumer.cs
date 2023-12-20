using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Handlers;

public interface ISinkConsumer
{
    IConsumer<byte[], byte[]> Subscribe(string connector, int taskId);

    Task<ConnectRecordBatch> Consume(IConsumer<byte[], byte[]> consumer, string connector, int taskId, bool consumeAll = false);
    
    void Commit(IConsumer<byte[], byte[]> consumer, CommandContext commandContext);
}