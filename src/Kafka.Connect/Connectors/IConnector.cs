using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Connect.Connectors;

public interface IConnector
{
    void Pause();
    void Resume(IDictionary<string, string> payload);
    Task Execute(string connector, CancellationTokenSource cts);
    bool IsPaused { get; }
    bool IsStopped { get; }
}