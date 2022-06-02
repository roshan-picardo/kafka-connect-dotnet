using System;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Handlers
{
    public interface IRetriableHandler
    {
        Task<SinkRecordBatch> Retry(Func<Task<SinkRecordBatch>> action, string connector);

        Task<SinkRecordBatch> Retry(Func<SinkRecordBatch, Task<SinkRecordBatch>> handler, SinkRecordBatch batch, string connector);

    }
}