using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Plugin;

public interface ISourceHandler
{
    Task<SinkRecordBatch> Get(string connector, int taskId);
}

public class SourceHandler : ISourceHandler
{
    public Task<SinkRecordBatch> Get(string connector, int taskId)
    {
        throw new System.NotImplementedException();
    }
}