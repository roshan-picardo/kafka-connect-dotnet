using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Handlers;

public interface ISourceProducer
{
    IProducer<byte[], byte[]> GetProducer(string connector, int taskId);
    Task Produce(IProducer<byte[], byte[]> producer, string connector, int taskId, ConnectRecordBatch batch);
    Task Produce(IProducer<byte[], byte[]> producer, CommandRecord sourceCommand);
}