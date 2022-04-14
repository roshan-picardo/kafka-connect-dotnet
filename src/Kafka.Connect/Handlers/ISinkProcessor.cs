using System.Threading.Tasks;
using Kafka.Connect.Config;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Handlers
{
    public interface ISinkProcessor
    {
        Task Process(SinkRecordBatch batch, string connector);
        Task Sink(SinkRecordBatch batch, string connector);
    }
}