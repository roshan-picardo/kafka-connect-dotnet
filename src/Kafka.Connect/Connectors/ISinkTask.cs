using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Connect.Connectors
{
    public interface ISinkTask
    {
        //Task Start(ConnectorConfig taskConfig, CancellationTokenSource cts = null);
        Task Execute(string connector, int taskId, CancellationToken cancellationToken);
    }
}