using System.Threading.Tasks;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Plugin;

public class SourceHandler : ISourceHandler
{
    private readonly ILogger<SourceHandler> _logger;

    public SourceHandler(ILogger<SourceHandler> logger)
    {
        _logger = logger;
    }
    public async Task<ConnectRecordBatch> Get(string connector, int taskId)
    {
        using (_logger.Track("Invoking Get"))
        {
            var batch = new ConnectRecordBatch(connector);

            // lets read from database
            
            return batch;
        }
    }
}