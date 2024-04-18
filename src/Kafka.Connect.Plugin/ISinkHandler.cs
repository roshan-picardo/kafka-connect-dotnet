using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Plugin;

public interface ISinkHandler
{
    Task Startup(string connector);
    Task Cleanup(string connector);
    bool Is(string connector, string plugin, string handler);
    Task Put(IEnumerable<ConnectRecord> models, string connector, int taskId);
}