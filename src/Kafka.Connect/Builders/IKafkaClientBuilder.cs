using Confluent.Kafka;

namespace Kafka.Connect.Builders
{
    public interface IKafkaClientBuilder
    {
        IConsumer<byte[], byte[]> GetConsumer(string connector, int taskId);
        IProducer<byte[], byte[]> GetProducer(string connector);
        IProducer<byte[], byte[]> GetProducer(ProducerConfig producerConfig);
        IAdminClient GetAdminClient(string connector = null);
    }
}