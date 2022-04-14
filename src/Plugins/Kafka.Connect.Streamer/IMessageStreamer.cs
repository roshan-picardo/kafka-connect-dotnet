using System.Threading.Tasks;
using Kafka.Connect.Config;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Handlers
{
    public interface IMessageStreamer
    {
        Task<SinkRecordBatch> Enrich(SinkRecordBatch batch, ConnectorConfig config);
        Task<SinkRecordBatch> Publish(SinkRecordBatch batch, ConnectorConfig config);
    }
}