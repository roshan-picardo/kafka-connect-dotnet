using System;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Handlers
{
    public interface IRetriableHandler
    {
        Task<ConnectRecordBatch> Retry(Func<Task<ConnectRecordBatch>> action, string connector);

        Task<ConnectRecordBatch> Retry(Func<ConnectRecordBatch, Task<ConnectRecordBatch>> handler, ConnectRecordBatch batch, string connector);

    }
}