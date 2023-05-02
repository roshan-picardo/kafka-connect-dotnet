using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Connect.Connectors
{
    public interface ISinkTask
    {
        Task Execute(string connector, int taskId, CancellationTokenSource cts);
        bool IsPaused { get; }
        bool IsStopped { get; }
    }
}