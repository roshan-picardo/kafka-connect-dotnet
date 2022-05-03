using Confluent.Kafka;

namespace Kafka.Connect.Builders
{
    public interface IConsumerBuilder
    {
        IConsumer<byte[], byte[]> Create();
        void AttachPartitionChangeEvents(string connector, int taskId);
    }
}