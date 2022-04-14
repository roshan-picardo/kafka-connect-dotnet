using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Tokens;

namespace Kafka.Connect.Connectors
{
    public interface IConnector
    {
        //public Task Start(Config.ConnectorConfig configuration,  CancellationTokenSource cts, PauseTokenSource pts);
        public Task Pause();
        public Task Resume(IDictionary<string, string> payload);
        public Task Restart(int? delay, IDictionary<string, string> payload);
        public Task Execute(string connector,  PauseTokenSource pts, CancellationToken cancellationToken);
    }
}