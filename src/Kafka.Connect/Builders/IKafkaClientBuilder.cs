using Confluent.Kafka;
using Kafka.Connect.Config;

namespace Kafka.Connect.Builders
{
    public interface IKafkaClientBuilder
    {
        IConsumer<byte[], byte[]> GetConsumer(string connector);
        IProducer<byte[], byte[]> GetProducer(string connector);

        IConsumer<byte[], byte[]> GetConsumer(ConsumerConfig consumerConfig);
        IAdminClient GetAdminClient(ConsumerConfig connectorConfig);
        
        IProducer<byte[], byte[]> GetProducer(ConnectorConfig connectorConfig);
        IProducer<byte[], byte[]> GetProducer(PublisherConfig publisherConfig);

        void AttachPartitionChangeEvents(string connector, int taskId);
    }
}