using Confluent.Kafka;

namespace Kafka.Connect.Builders
{
    public interface IKafkaClientBuilder
    {
        IConsumer<byte[], byte[]> GetConsumer(string connector, int taskId);
        IProducer<byte[], byte[]> GetProducer(string connector);
        IAdminClient GetAdminClient(string connector = null);
    }
}