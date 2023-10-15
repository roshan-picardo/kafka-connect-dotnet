using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Plugin;

public interface ISourceHandler
{
    Task<ConnectRecordBatch> Get(string connector, int taskId);
}

public class SourceHandler : ISourceHandler
{
    public Task<ConnectRecordBatch> Get(string connector, int taskId)
    {
        throw new System.NotImplementedException();
    }
}