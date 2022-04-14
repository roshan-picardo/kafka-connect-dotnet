using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Config;

namespace Kafka.Connect.Connectors
{
    public interface ISinkTask
    {
        //Task Start(ConnectorConfig taskConfig, CancellationTokenSource cts = null);
        Task Execute(string connector, int taskId, CancellationToken cancellationToken);
    }
}