using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Models;

namespace Kafka.Connect.Connectors
{
    public interface IConnectDeadLetter
    {
        Task Send(IEnumerable<ConnectSinkRecord> sinkRecords, Exception exception, string connector);
    }
}