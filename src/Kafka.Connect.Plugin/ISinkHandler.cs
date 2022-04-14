using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Plugin
{
    public interface ISinkHandler
    {
        Task<SinkRecordBatch> Put(SinkRecordBatch sinkRecordBatch);

        Task Startup(string connector);

        Task Cleanup(string connector);

        bool IsOfType(string plugin, string type);
    }
}