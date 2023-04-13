using Confluent.Kafka;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Providers;

namespace Kafka.Connect.Builders
{
    public class KafkaClientBuilder : IKafkaClientBuilder
    {
        private readonly ILogger<KafkaClientBuilder> _logger;
        private readonly IConfigurationProvider _configurationProvider;
        private readonly IKafkaClientEventHandler _kafkaClientEventHandler;

        public KafkaClientBuilder(ILogger<KafkaClientBuilder> logger, IConfigurationProvider configurationProvider, IKafkaClientEventHandler kafkaClientEventHandler)
        {
            _logger = logger;
            _configurationProvider = configurationProvider;
            _kafkaClientEventHandler = kafkaClientEventHandler;
        }

        public IConsumer<byte[], byte[]> GetConsumer(string connector, int taskId)
        {
            using (_logger.Track("Creating message consumer."))
            {
                return new ConsumerBuilder<byte[], byte[]>(_configurationProvider.GetConsumerConfig(connector))
                    .SetErrorHandler((_, error) => _kafkaClientEventHandler.HandleError(error))
                    .SetLogHandler((_, message) => _kafkaClientEventHandler.HandleLogMessage(message))
                    .SetStatisticsHandler((_, stats) => _kafkaClientEventHandler.HandleStatistics(stats))
                    .SetPartitionsAssignedHandler((_, partitions) =>
                        _kafkaClientEventHandler.HandlePartitionAssigned(connector, taskId, partitions))
                    .SetPartitionsRevokedHandler((_, offsets) =>
                        _kafkaClientEventHandler.HandlePartitionRevoked(connector, taskId, offsets))
                    .SetOffsetsCommittedHandler((_, offsets) => _kafkaClientEventHandler.HandleOffsetCommitted(offsets))
                    .Build();
            }
        }

        public IProducer<byte[], byte[]> GetProducer(string connector)
        {
            return GetProducer(_configurationProvider.GetProducerConfig(connector));
        }

        public IProducer<byte[], byte[]> GetProducer(ProducerConfig producerConfig)
        {
            using (_logger.Track("Creating message producer."))
            {
                return new ProducerBuilder<byte[], byte[]>(producerConfig)
                    .SetLogHandler((_, message) => _kafkaClientEventHandler.HandleLogMessage(message))
                    .SetErrorHandler((_, error) => _kafkaClientEventHandler.HandleError(error))
                    .SetStatisticsHandler((_, message) => _kafkaClientEventHandler.HandleStatistics(message))
                    .Build();
            }
        }

        public IAdminClient GetAdminClient(string connector = null)
        {
            using (_logger.Track("Creating Kafka admin client."))
            {
                return new AdminClientBuilder(_configurationProvider.GetConsumerConfig(connector))
                    .SetErrorHandler((_, error) => _kafkaClientEventHandler.HandleError(error))
                    .SetLogHandler((_, message) => _kafkaClientEventHandler.HandleLogMessage(message))
                    .SetStatisticsHandler((_, message) => _kafkaClientEventHandler.HandleStatistics(message))
                    .Build();
            }
        }
    }
}