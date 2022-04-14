using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Connectors;

namespace Kafka.Connect
{
    public interface IWorker
    {
        Task PauseAsync();
        Task ResumeAsync();
        Task RestartAsync(int? delayMs);
        Task Execute(CancellationToken stoppingToken);
        IConnector GetConnector(string name);
    }
}