using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Models;

namespace Kafka.Connect.Handlers;

public interface ISinkConsumer
{
    IConsumer<byte[], byte[]> Subscribe(string connector, int taskId);

    Task<IList<SinkRecord>> Consume(IConsumer<byte[], byte[]> consumer, string connector, int taskId, bool consumeAll = false);
    
    void Commit(IConsumer<byte[], byte[]> consumer, CommandContext commandContext);
}