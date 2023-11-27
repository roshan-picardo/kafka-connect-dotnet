using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Plugin
{
    public interface ISinkHandler
    {
        Task<ConnectRecordBatch> Put(ConnectRecordBatch connectRecordBatch, string connector, int taskId, int parallelism = 100);
        Task Startup(string connector);
        Task Cleanup(string connector);
        bool Is(string connector, string plugin, string handler);
    }
}