using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Kafka.Connect.Connectors;
using Microsoft.Extensions.Logging;
using Microsoft.VisualBasic;
using Microsoft.VisualBasic.CompilerServices;

namespace Kafka.Connect.Builders
{
    public class KafkaClientEventHandler : IKafkaClientEventHandler
    {
        private readonly ILogger<KafkaClientEventHandler> _logger;
        private readonly IExecutionContext _executionContext;

        public KafkaClientEventHandler(ILogger<KafkaClientEventHandler> logger, IExecutionContext executionContext)
        {
            _logger = logger;
            _executionContext = executionContext;
        }
        
        
        public void HandleError(Error error)
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

        public void HandleLogMessage(LogMessage log)
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
                    var info = LikeOperator.LikeString(log.Message,
                        "* Configuration property * is a consumer property and will be ignored by this producer instance",
                        CompareMethod.Text);
                    _logger.Log(info ? LogLevel.Trace : LogLevel.Warning, "{@Log}", log);
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

        public void HandleStatistics(string stats)
        {
            _logger.LogDebug("{@Log}", new {Message = "Statistics", Stats = stats});
        }

        public void HandlePartitionAssigned(string connector, int taskId, IList<TopicPartition> partitions)
        {
            if (!(partitions?.Any() ?? false))
            {
                _logger.LogTrace("{@Log}", new {Message = "No partitions assigned."});
                return;
            }
            _executionContext.AssignPartitions(connector, taskId, partitions);
            _logger.LogDebug("{@Log}",
                new
                {
                    Message = "Assigned partitions.",
                    Partitions = partitions.Select(p => $"{{topic:{p.Topic}}} - {{partition:{p.Partition.Value}}}").ToList()
                });
        }

        public void HandlePartitionRevoked(string connector, int taskId, IList<TopicPartitionOffset> offsets)
        {
            if (!(offsets?.Any() ?? false))
            {
                _logger.LogTrace("{@Log}", new {Message = "No partitions revoked."});
                return;
            }
            _executionContext.RevokePartitions(connector, taskId, offsets.Select(o=>o.TopicPartition).ToList());
            _logger.LogDebug("{@Log}",
                new
                {
                    Message = "Revoked partitions.",
                    Partitions = offsets.Select(p => $"{{topic={p.Topic}}} - {{partition={p.Partition.Value}}} - {{offset:{p.Offset.Value}}}").ToList()
                });
        }

        public void HandleOffsetCommitted(CommittedOffsets offsets)
        {
            if (offsets.Error?.Code != ErrorCode.NoError)
            {
                _logger.LogWarning("{@Log}",
                    new
                    {
                        Message = "Error committing offsets.", Reason = offsets.Error,
                        Offsets = offsets.Offsets?.Select(o =>
                            $"{{topic={o.Topic}}} - {{partition={o.Partition.Value}}} - {{offset:{o.Offset.Value}}} - {{status:{o.Error.Code}}}").ToList()
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
                    Offsets = offsets.Offsets.Select(o => $"{{topic={o.Topic}}} - {{partition={o.Partition.Value}}} - {{offset:{o.Offset.Value}}} - {{status:{o.Error.Code}}}").ToList()
                });
        }
    }
}