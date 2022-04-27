using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Connectors
{
    public interface IConnectDeadLetter
    {
        Task Send(IEnumerable<SinkRecord> sinkRecords, Exception exception, string connector);
    }
}