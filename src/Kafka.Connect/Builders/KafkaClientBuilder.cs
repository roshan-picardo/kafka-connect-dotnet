using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin.Logging;
using Microsoft.Extensions.Logging;
using Microsoft.VisualBasic;
using Microsoft.VisualBasic.CompilerServices;

namespace Kafka.Connect.Builders
{
    public class KafkaClientBuilder : IKafkaClientBuilder
    {
        private readonly IExecutionContext _executionContext;
        private readonly Func<string, IConsumerBuilder> _consumerBuilderFactory;
        private readonly ILogger<KafkaClientBuilder> _logger;
        private readonly IConfigurationProvider _configurationProvider;
        private Action<IEnumerable<TopicPartition>> OnPartitionAssigned { get; set; }
        private Action<IEnumerable<TopicPartition>> OnPartitionRevoked { get; set; }

        public KafkaClientBuilder(ILogger<KafkaClientBuilder> logger, IExecutionContext executionContext, Func<string, IConsumerBuilder> consumerBuilderFactory, IConfigurationProvider configurationProvider)
        {
            _executionContext = executionContext;
            _consumerBuilderFactory = consumerBuilderFactory;
            _logger = logger;
            _configurationProvider = configurationProvider;
        }

        public IConsumer<byte[], byte[]> GetConsumer(string connector)
        {
            return _consumerBuilderFactory(connector).Create();
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

        public IAdminClient GetAdminClient(string connector = null)
        {
            return _logger.Timed("Creating admin client")
                .Execute(() => new AdminClientBuilder(_configurationProvider.GetConsumerConfig(connector))
                    .SetErrorHandler((_, error) => HandleError(error))
                    .SetLogHandler((_, message) => HandleLogMessage(message))
                    .SetStatisticsHandler((_, message) => HandleStatistics(message))
                    .Build());
        }

        public void AttachPartitionChangeEvents(string connector, int taskId)
        {
            OnPartitionAssigned = partitions => _executionContext.AssignPartitions(connector, taskId, partitions);
            OnPartitionRevoked = partitions => _executionContext.RevokePartitions(connector, taskId, partitions);
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