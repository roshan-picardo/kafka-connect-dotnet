using System.Collections.Concurrent;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Plugin;

public interface ISinkHandler
{
    Task<ConnectRecordModel> BuildModels(ConnectRecord record, string connector);
    Task Startup(string connector);
    Task Cleanup(string connector);
    bool Is(string connector, string plugin, string handler);
    Task Put(BlockingCollection<ConnectRecordModel> models, string connector, int taskId);
}