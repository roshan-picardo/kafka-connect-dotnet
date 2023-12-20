using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Plugin;

public interface ISourceHandler
{
    Task<ConnectRecordBatch> Get(string connector, int taskId);
}