using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Connect;

public interface IWorker
{
    public Task Add(string connector);
    public Task Remove(string connector);
    public Task Refresh(string connector, bool isDelete = false);
    Task Pause();
    Task Resume();
    Task Execute(CancellationTokenSource cts);
    bool IsPaused { get; }
    bool IsStopped { get; }
    bool IsRunning(string connector);
}