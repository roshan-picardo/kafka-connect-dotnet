using System;
using Confluent.Kafka;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin.Logging;
using Microsoft.Extensions.Logging;

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
            return _logger.Timed("Creating consumer.")
                .Execute(() => new ConsumerBuilder<byte[], byte[]>(_configurationProvider.GetConsumerConfig(connector))
                    .SetErrorHandler((_, error) => _kafkaClientEventHandler.HandleError(error)) 
                    .SetLogHandler((_, message) => _kafkaClientEventHandler.HandleLogMessage(message))
                    .SetStatisticsHandler((_, stats) => _kafkaClientEventHandler.HandleStatistics(stats))
                    .SetPartitionsAssignedHandler((_, partitions) => _kafkaClientEventHandler.HandlePartitionAssigned(connector, taskId, partitions))
                    .SetPartitionsRevokedHandler((_, offsets) => _kafkaClientEventHandler.HandlePartitionRevoked(connector, taskId, offsets))
                    .SetOffsetsCommittedHandler((_, offsets) => _kafkaClientEventHandler.HandleOffsetCommitted(offsets))
                    .Build());
        }

        public IProducer<byte[], byte[]> GetProducer(string connector)
        {
            return _logger.Timed("Creating producer.")
                .Execute(() => new ProducerBuilder<byte[], byte[]>(_configurationProvider.GetProducerConfig(connector))
                    .SetLogHandler((_, message) => _kafkaClientEventHandler.HandleLogMessage(message))
                    .SetErrorHandler((_, error) => _kafkaClientEventHandler.HandleError(error))
                    .SetStatisticsHandler((_, message) => _kafkaClientEventHandler.HandleStatistics(message))
                    .Build());
        }

        public IAdminClient GetAdminClient(string connector = null)
        {
            return _logger.Timed("Creating admin client")
                .Execute(() => new AdminClientBuilder(_configurationProvider.GetConsumerConfig(connector))
                    .SetErrorHandler((_, error) => _kafkaClientEventHandler.HandleError(error))
                    .SetLogHandler((_, message) => _kafkaClientEventHandler.HandleLogMessage(message))
                    .SetStatisticsHandler((_, message) => _kafkaClientEventHandler.HandleStatistics(message))
                    .Build());
        }
    }
}