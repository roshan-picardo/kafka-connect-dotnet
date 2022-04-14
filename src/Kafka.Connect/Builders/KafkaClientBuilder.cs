using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Kafka.Connect.Config;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin.Logging;
using Microsoft.Extensions.Logging;
using Microsoft.VisualBasic;
using Microsoft.VisualBasic.CompilerServices;
using ConnectorConfig = Kafka.Connect.Config.ConnectorConfig;

namespace Kafka.Connect.Builders
{
    public class KafkaClientBuilder : IKafkaClientBuilder
    {
        private readonly IExecutionContext _executionContext;
        private readonly ILogger<KafkaClientBuilder> _logger;
        private readonly IConfigurationProvider _configurationProvider;
        private Action<IEnumerable<TopicPartition>> OnPartitionAssigned { get; set; }
        private Action<IEnumerable<TopicPartition>> OnPartitionRevoked { get; set; }

        public KafkaClientBuilder(IExecutionContext executionContext, ILogger<KafkaClientBuilder> logger, IConfigurationProvider configurationProvider)
        {
            _executionContext = executionContext;
            _logger = logger;
            _configurationProvider = configurationProvider;
        }

        public IConsumer<byte[], byte[]> GetConsumer(string connector)
        {
            return _logger.Timed("Creating consumer.")
                .Execute(() => new ConsumerBuilder<byte[], byte[]>(_configurationProvider.GetConsumerConfig(connector))
                    .SetErrorHandler((_, error) => HandleError(error))
                    .SetLogHandler((_, message) => HandleLogMessage(message))
                    .SetStatisticsHandler((_, message) => HandleStatistics(message))
                    .SetPartitionsAssignedHandler(HandlePartitionAssigned)
                    .SetPartitionsRevokedHandler(HandlePartitionRevoked)
                    .SetOffsetsCommittedHandler(HandleOffsetCommitted)
                    .Build());
        }

        public IProducer<byte[], byte[]> GetProducer(string connector)
        {
            return _logger.Timed("Creating producer.")
                .Execute(() => new ProducerBuilder<byte[], byte[]>(_configurationProvider.GetProducerConfig(connector))
                    .SetLogHandler((_, message) => HandleLogMessage(message))
                    .SetErrorHandler((_, error) => HandleError(error))
                    .SetStatisticsHandler((_, message) => HandleStatistics(message))
                    .Build());
        }

        public IConsumer<byte[], byte[]> GetConsumer(ConsumerConfig consumerConfig)
        {
            return _logger.Timed("Creating consumer.")
                .Execute(() => new ConsumerBuilder<byte[], byte[]>(consumerConfig)
                    .SetErrorHandler((_, error) => HandleError(error))
                    .SetLogHandler((_, message) => HandleLogMessage(message))
                    .SetStatisticsHandler((_, message) => HandleStatistics(message))
                    .SetPartitionsAssignedHandler(HandlePartitionAssigned)
                    .SetPartitionsRevokedHandler(HandlePartitionRevoked)
                    .SetOffsetsCommittedHandler(HandleOffsetCommitted)
                    .Build());
        }

        public IAdminClient GetAdminClient(ConsumerConfig connectorConfig)
        {
            return _logger.Timed("Creating admin client")
                .Execute(() => new AdminClientBuilder(connectorConfig)
                    .SetErrorHandler((_, error) => HandleError(error))
                    .SetLogHandler((_, message) => HandleLogMessage(message))
                    .SetStatisticsHandler((_, message) => HandleStatistics(message))
                    .Build());
        }

        public IProducer<byte[], byte[]> GetProducer(ConnectorConfig connectorConfig)
        {
            return _logger.Timed("Creating producer.")
                .Execute(() => new ProducerBuilder<byte[], byte[]>(connectorConfig)
                    .SetLogHandler((_, message) => HandleLogMessage(message))
                    .SetErrorHandler((_, error) => HandleError(error))
                    .SetStatisticsHandler((_, message) => HandleStatistics(message))
                    .Build());
        }
        
        public IProducer<byte[], byte[]> GetProducer(PublisherConfig publisherConfig)
        {
            return _logger.Timed("Creating producer.")
                .Execute(() => new ProducerBuilder<byte[], byte[]>(publisherConfig)
                    .SetLogHandler((_, message) => HandleLogMessage(message))
                    .SetErrorHandler((_, error) => HandleError(error))
                    .SetStatisticsHandler((_, message) => HandleStatistics(message))
                    .Build());
        }

        public void AttachPartitionChangeEvents(string connector, int taskId)
        {
            OnPartitionAssigned = partitions => _executionContext.AssignPartitions(connector, taskId, partitions);
            OnPartitionRevoked = partitions => _executionContext.RevokePartitions(connector, taskId, partitions);
        }
        
        private void HandlePartitionAssigned(IConsumer<byte[], byte[]> consumer, IEnumerable<TopicPartition> partitions)
        {
            var topicPartitions = partitions as TopicPartition[] ?? partitions.ToArray();
            OnPartitionAssigned?.Invoke(topicPartitions.ToList());
            _logger.LogDebug("{@Log}",
                new
                {
                    Message = "Topic partitions assigned.",
                    Partitions = topicPartitions.Select(p => new {p.Topic, Partition = p.Partition.Value})
                });
        }  
        
        private void HandlePartitionRevoked(IConsumer<byte[], byte[]> consumer, IEnumerable<TopicPartitionOffset> offsets)
        {
            var topicPartitionOffsets = offsets as TopicPartitionOffset[] ?? offsets.ToArray();
            OnPartitionRevoked?.Invoke( topicPartitionOffsets.Select(o=>o.TopicPartition));
            _logger.LogDebug("{@Log}",
                new
                {
                    Message = "Topic partitions revoked.",
                    Partitions = topicPartitionOffsets.Select(p => new {p.Topic, Partition = p.Partition.Value, Offset = p.Offset.Value})
                });
        }

        private void HandleOffsetCommitted(IConsumer<byte[], byte[]> consumer, CommittedOffsets offsets)
        {
            _logger.LogDebug("{@Log}",
                new
                {
                    Message = "Offsets committed.",
                    Offsets = offsets.Offsets.Select(o => new {o.Topic, Partition = o.Partition.Value, Offset = o.Offset.Value, o.Error.Code})
                });
        }

        private void HandleStatistics(string stats)
        {
            _logger.LogDebug("{@Log}", new {Message = "Statistics", Stats = stats});
        }
        private void HandleError(Error error)
        {
            if (error.IsFatal)
            {
                _logger.LogCritical("{@Log}", error);
            }
            else if (error.IsError || error.IsBrokerError || error.IsLocalError)
            {
                _logger.LogError("{@Log}", error);
            }
            else
            {
                _logger.LogWarning("{@Log}", error);
            }
        }
        
        private void HandleLogMessage(LogMessage log)
        {
            const string consumerPropertyWarning = "* Configuration property * is a consumer property and will be ignored by this producer instance";
            switch (log.Level)
            {
                case SyslogLevel.Emergency:
                case SyslogLevel.Critical:
                    _logger.LogCritical("{@Log}", log);
                    break;
                case SyslogLevel.Alert:
                case SyslogLevel.Error:
                    _logger.LogError("{@Log}", log);
                    break;
                case SyslogLevel.Warning
                    when LikeOperator.LikeString(log.Message, consumerPropertyWarning, CompareMethod.Text):
                    _logger.LogTrace("{@Log}", log);
                    break;
                case SyslogLevel.Notice:
                case SyslogLevel.Warning:
                    _logger.LogWarning("{@Log}", log);
                    break;
                case SyslogLevel.Info:
                    _logger.LogInformation("{@Log}", log);
                    break;
                case SyslogLevel.Debug:
                    _logger.LogDebug("{@Log}", log);
                    break;
                default:
                    _logger.LogWarning("{@Log}", log);
                    break;
            }
        }
    }
}