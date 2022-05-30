using Confluent.Kafka;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Providers;

namespace Kafka.Connect.Builders
{
    public class KafkaClientBuilder : IKafkaClientBuilder
    {
        private readonly IConfigurationProvider _configurationProvider;
        private readonly IKafkaClientEventHandler _kafkaClientEventHandler;

        public KafkaClientBuilder(IConfigurationProvider configurationProvider, IKafkaClientEventHandler kafkaClientEventHandler)
        {
            _configurationProvider = configurationProvider;
            _kafkaClientEventHandler = kafkaClientEventHandler;
        }

        [OperationLog("Creating message consumer.")]
        public IConsumer<byte[], byte[]> GetConsumer(string connector, int taskId)
        {
            return new ConsumerBuilder<byte[], byte[]>(_configurationProvider.GetConsumerConfig(connector))
                .SetErrorHandler((_, error) => _kafkaClientEventHandler.HandleError(error))
                .SetLogHandler((_, message) => _kafkaClientEventHandler.HandleLogMessage(message))
                .SetStatisticsHandler((_, stats) => _kafkaClientEventHandler.HandleStatistics(stats))
                .SetPartitionsAssignedHandler((_, partitions) => _kafkaClientEventHandler.HandlePartitionAssigned(connector, taskId, partitions))
                .SetPartitionsRevokedHandler((_, offsets) => _kafkaClientEventHandler.HandlePartitionRevoked(connector, taskId, offsets))
                .SetOffsetsCommittedHandler((_, offsets) => _kafkaClientEventHandler.HandleOffsetCommitted(offsets))
                .Build();
        }

        [OperationLog("Creating message producer.")]
        public IProducer<byte[], byte[]> GetProducer(string connector)
        {
            return new ProducerBuilder<byte[], byte[]>(_configurationProvider.GetProducerConfig(connector))
                .SetLogHandler((_, message) => _kafkaClientEventHandler.HandleLogMessage(message))
                .SetErrorHandler((_, error) => _kafkaClientEventHandler.HandleError(error))
                .SetStatisticsHandler((_, message) => _kafkaClientEventHandler.HandleStatistics(message))
                .Build();
        }

        [OperationLog("Creating Kafka admin client.")]
        public IAdminClient GetAdminClient(string connector = null)
        {
            return new AdminClientBuilder(_configurationProvider.GetConsumerConfig(connector))
                .SetErrorHandler((_, error) => _kafkaClientEventHandler.HandleError(error))
                .SetLogHandler((_, message) => _kafkaClientEventHandler.HandleLogMessage(message))
                .SetStatisticsHandler((_, message) => _kafkaClientEventHandler.HandleStatistics(message))
                .Build();
        }
    }
}