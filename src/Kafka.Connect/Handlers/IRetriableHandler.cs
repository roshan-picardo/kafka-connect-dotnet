using System;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Handlers
{
    public interface IRetriableHandler
    {
        Task<SinkRecordBatch> Retry(Func<Task<SinkRecordBatch>> action, int attempts, int delayTimeoutMs);

        Task Retry(Func<SinkRecordBatch, Task<Guid>> handler, SinkRecordBatch consumedBatch, int attempts,
            int delayTimeoutMs);
        
        Task<SinkRecordBatch> Retry(Func<SinkRecordBatch, Task<SinkRecordBatch>> handler, SinkRecordBatch consumedBatch, int attempts,
            int delayTimeoutMs);

        Task<SinkRecordBatch> Retry(Func<Task<SinkRecordBatch>> handler, SinkRecordBatch batch, string connector);

    }
}