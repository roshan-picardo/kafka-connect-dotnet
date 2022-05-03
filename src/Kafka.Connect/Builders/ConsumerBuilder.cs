using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin.Logging;
using Microsoft.Extensions.Logging;

namespace Kafka.Connect.Builders
{
    public class ConsumerBuilder : ConsumerBuilder<byte[], byte[]>, IConsumerBuilder
    {
        private readonly ILogger<ConsumerBuilder> _logger;
        private readonly IExecutionContext _executionContext;
        private Action<IEnumerable<TopicPartition>> OnPartitionAssigned { get; set; }
        private Action<IEnumerable<TopicPartition>> OnPartitionRevoked { get; set; }

        public ConsumerBuilder(ILogger<ConsumerBuilder> logger, IConfigurationProvider configurationProvider, IExecutionContext executionContext, string connector) : base(configurationProvider.GetConsumerConfig(connector))
        {
            _logger = logger;
            _executionContext = executionContext;
        }

        public IConsumer<byte[], byte[]> Create()
        {
            return _logger.Timed("Creating consumer.")
                .Execute(() => SetErrorHandler(HandleError)
                    .SetLogHandler(HandleLogMessage)
                    .SetStatisticsHandler(HandleStatistics)
                    .SetPartitionsAssignedHandler(HandlePartitionAssigned)
                    .SetPartitionsRevokedHandler(HandlePartitionRevoked)
                    .SetOffsetsCommittedHandler(HandleOffsetCommitted)
                    .Build());
        }

        public void AttachPartitionChangeEvents(string connector, int taskId)
        {
            OnPartitionAssigned = partitions => _executionContext.AssignPartitions(connector, taskId, partitions);
            OnPartitionRevoked = partitions => _executionContext.RevokePartitions(connector, taskId, partitions);
        }

        internal new Action<IConsumer<byte[], byte[]>, Error> ErrorHandler => base.ErrorHandler;
        internal new Action<IConsumer<byte[], byte[]>, LogMessage> LogHandler => base.LogHandler;
        internal new Action<IConsumer<byte[], byte[]>, string> StatisticsHandler => base.StatisticsHandler;
        internal new Func<IConsumer<byte[], byte[]>, List<TopicPartition>, IEnumerable<TopicPartitionOffset>> PartitionsAssignedHandler => base.PartitionsAssignedHandler;
        internal new Func<IConsumer<byte[], byte[]>, List<TopicPartitionOffset>, IEnumerable<TopicPartitionOffset>> PartitionsRevokedHandler => base.PartitionsRevokedHandler;
        internal new Action<IConsumer<byte[], byte[]>, CommittedOffsets> OffsetsCommittedHandler => base.OffsetsCommittedHandler;

        private void HandleError(IConsumer<byte[], byte[]> consumer, Error error)
        {
            if (error.IsFatal)
            {
                _logger.LogCritical("{@Log}", error);
            }
            else if ( error.IsBrokerError || error.IsLocalError || error.IsError)
            {
                _logger.LogError("{@Log}", error);
            }
            else
            {
                _logger.LogDebug("{@Log}", error);
            }
        }
        
        private void HandleLogMessage(IConsumer<byte[], byte[]> consumer,LogMessage log)
        {
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
        
        private void HandleStatistics(IConsumer<byte[], byte[]> consumer, string stats)
        {
            _logger.LogDebug("{@Log}", new {Message = "Statistics", Stats = stats});
        }
        
        private void HandlePartitionAssigned(IConsumer<byte[], byte[]> consumer, IList<TopicPartition> partitions)
        {
            if (!(partitions?.Any() ?? false))
            {
                _logger.LogTrace("{@Log}", new {Message = "No partitions assigned."});
                return;
            }
            OnPartitionAssigned?.Invoke(partitions);
            _logger.LogDebug("{@Log}",
                new
                {
                    Message = "Assigned partitions.",
                    Partitions = partitions.Select(p => $"{{topic:{p.Topic}}} - {{partition:{p.Partition.Value}}}")
                });
        }  
        
        private void HandlePartitionRevoked(IConsumer<byte[], byte[]> consumer, IList<TopicPartitionOffset> offsets)
        {
            if (!(offsets?.Any() ?? false))
            {
                _logger.LogTrace("{@Log}", new {Message = "No partitions revoked."});
                return;
            }
            OnPartitionRevoked?.Invoke(offsets.Select(o => o.TopicPartition));
            _logger.LogDebug("{@Log}",
                new
                {
                    Message = "Revoked partitions.",
                    Partitions = offsets.Select(p => $"{{topic={p.Topic}}} - {{partition={p.Partition.Value}}} - {{offset:{p.Offset.Value}}}")
                });
        }

        private void HandleOffsetCommitted(IConsumer<byte[], byte[]> consumer, CommittedOffsets offsets)
        {
            if (offsets.Error?.Code != ErrorCode.NoError)
            {
                _logger.LogWarning("{@Log}",
                    new
                    {
                        Message = "Error committing offsets.", Reason = offsets.Error,
                        Offsets = offsets.Offsets?.Select(o =>
                            $"{{topic={o.Topic}}} - {{partition={o.Partition.Value}}} - {{offset:{o.Offset.Value}}} - {{status:{o.Error.Code}}}")
                    });
                return;
            }

            if (!(offsets.Offsets?.Any() ?? false))
            {
                _logger.LogTrace("{@Log}", new {Message = "No offsets committed."});
                return;
            }
            
            _logger.LogDebug("{@Log}",
                new
                {
                    Message = "Offsets committed.",
                    Offsets = offsets.Offsets.Select(o => $"{{topic={o.Topic}}} - {{partition={o.Partition.Value}}} - {{offset:{o.Offset.Value}}} - {{status:{o.Error.Code}}}")
                });
        }
    }
}