using Confluent.Kafka;

namespace Kafka.Connect.Handlers;

public interface ISourceProducer
{
    IProducer<byte[], byte[]> GetProducer(string connector, int taskId);
}