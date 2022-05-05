using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Config;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Handlers
{
    public interface ISinkConsumer
    {
        IConsumer<byte[], byte[]> Subscribe(string connector, int taskId);

        Task<SinkRecordBatch> Consume(IConsumer<byte[], byte[]> consumer, string connector, int taskId);
    }
}