using Confluent.Kafka;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Providers;

namespace Kafka.Connect.Builders;

public class KafkaClientBuilder(
    ILogger<KafkaClientBuilder> logger,
    IConfigurationProvider configurationProvider,
    IKafkaClientEventHandler kafkaClientEventHandler) : IKafkaClientBuilder
{
    public IConsumer<byte[], byte[]> GetConsumer(string connector, int taskId)
    {
        using (logger.Track("Creating message consumer."))
        {
            return new ConsumerBuilder<byte[], byte[]>(configurationProvider.GetConsumerConfig(connector))
                .SetErrorHandler((_, error) => kafkaClientEventHandler.HandleError(error))
                .SetLogHandler((_, message) => kafkaClientEventHandler.HandleLogMessage(message))
                .SetStatisticsHandler((_, stats) => kafkaClientEventHandler.HandleStatistics(stats))
                .SetPartitionsAssignedHandler((_, partitions) =>
                    kafkaClientEventHandler.HandlePartitionAssigned(connector, taskId, partitions))
                .SetPartitionsRevokedHandler((_, offsets) =>
                    kafkaClientEventHandler.HandlePartitionRevoked(connector, taskId, offsets))
                .SetOffsetsCommittedHandler((_, offsets) => kafkaClientEventHandler.HandleOffsetCommitted(offsets))
                .Build();
        }
    }

    public IProducer<byte[], byte[]> GetProducer(string connector)
    {
        return GetProducer(configurationProvider.GetProducerConfig(connector));
    }

    public IProducer<byte[], byte[]> GetProducer(ProducerConfig producerConfig)
    {
        using (logger.Track("Creating message producer."))
        {
            return new ProducerBuilder<byte[], byte[]>(producerConfig)
                .SetLogHandler((_, message) => kafkaClientEventHandler.HandleLogMessage(message))
                .SetErrorHandler((_, error) => kafkaClientEventHandler.HandleError(error))
                .SetStatisticsHandler((_, message) => kafkaClientEventHandler.HandleStatistics(message))
                .Build();
        }
    }

    public IAdminClient GetAdminClient(string connector = null)
    {
        using (logger.Track("Creating Kafka admin client."))
        {
            return new AdminClientBuilder(configurationProvider.GetConsumerConfig(connector))
                .SetErrorHandler((_, error) => kafkaClientEventHandler.HandleError(error))
                .SetLogHandler((_, message) => kafkaClientEventHandler.HandleLogMessage(message))
                .SetStatisticsHandler((_, message) => kafkaClientEventHandler.HandleStatistics(message))
                .Build();
        }
    }
}