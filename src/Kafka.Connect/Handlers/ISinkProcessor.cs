using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Handlers
{
    public interface ISinkProcessor
    {
        Task Process(ConnectRecordBatch batch, string connector);
        Task<T> Process<T>(Kafka.Connect.Models.SinkRecord record, string connector);
        Task Sink(ConnectRecordBatch batch, string connector, int taskId);
    }
}