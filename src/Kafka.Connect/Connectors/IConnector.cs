using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Connect.Connectors
{
    public interface IConnector
    {
        Task Pause();
        Task Resume(IDictionary<string, string> payload);
        Task Restart(int? delay, IDictionary<string, string> payload);
        Task Execute(string connector, CancellationTokenSource cts);
        bool IsPaused { get; }
        bool IsStopped { get; }
    }
}