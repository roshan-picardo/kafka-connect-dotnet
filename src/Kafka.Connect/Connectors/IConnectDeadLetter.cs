using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Config;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Connectors
{
    public interface IConnectDeadLetter
    {
        public Task CreateTopic(ConnectorConfig connectorConfig);

        Task Send(IEnumerable<SinkRecord> sinkRecords, Exception exception, string connector);
    }
}