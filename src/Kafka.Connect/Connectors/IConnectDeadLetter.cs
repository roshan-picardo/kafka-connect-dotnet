using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Models;

namespace Kafka.Connect.Connectors
{
    public interface IConnectDeadLetter
    {
        Task Send(IEnumerable<ConnectRecord> sinkRecords, Exception exception, string connector);
    }
}