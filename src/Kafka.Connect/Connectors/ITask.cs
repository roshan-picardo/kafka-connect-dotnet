using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Connect.Connectors;

public interface ITask
{
    Task Execute(string connector, int taskId, CancellationTokenSource cts);
    bool IsPaused { get; }
    bool IsStopped { get; }
}

public interface ISinkTask : ITask
{
}

public interface ISourceTask : ITask
{
}
