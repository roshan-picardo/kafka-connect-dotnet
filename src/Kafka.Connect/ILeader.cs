using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Connect;

public interface ILeader
{
    Task Pause();
    Task Resume();
    Task Execute(CancellationTokenSource cts);
    bool IsPaused { get; }
    bool IsStopped { get; }
}