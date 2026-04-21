using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin.Logging;
using Microsoft.VisualBasic;
using Microsoft.VisualBasic.CompilerServices;

namespace Kafka.Connect.Builders;

public class KafkaClientEventHandler(
    ILogger<KafkaClientEventHandler> logger,
    IExecutionContext executionContext) : IKafkaClientEventHandler
{
    public void HandleError(Error error)
    {
        if (error.IsFatal)
        {
            logger.Critical(error.Reason, error);
        }
        else if ( error.IsBrokerError || error.IsLocalError || error.IsError)
        {
            logger.Error(error.Reason, error);
        }
        else
        {
            logger.Debug(error.Reason, error);
        }
    }

    public void HandleLogMessage(LogMessage log)
    {
        switch (log.Level)
        {
            case SyslogLevel.Emergency:
            case SyslogLevel.Critical:
                logger.Critical(log.Message, log);
                break;
            case SyslogLevel.Alert:
            case SyslogLevel.Error:
                logger.Error(log.Message, log);
                break;
            case SyslogLevel.Notice:
            case SyslogLevel.Warning:
                var info = LikeOperator.LikeString(log.Message,
                    "* Configuration property * is a consumer property and will be ignored by this producer instance",
                    CompareMethod.Text);
                if (info)
                {
                    logger.Trace(log.Message, log);
                }
                else
                {
                    logger.Warning(log.Message, log);
                }
                break;
            case SyslogLevel.Info:
                logger.Info(log.Message, log);
                break;
            case SyslogLevel.Debug:
                logger.Debug(log.Message, log);
                break;
            default:
                logger.Warning(log.Message, log);
                break;
        }
    }

    public void HandleStatistics(string stats)
    {
        logger.Debug("Statistics" ,new { Stats = stats });
    }

    public void HandlePartitionAssigned(string connector, int taskId, IList<TopicPartition> partitions)
    {
        if (!(partitions?.Any() ?? false))
        {
            logger.Trace("No partitions assigned.");
            return;
        }
        executionContext.AssignPartitions(connector, taskId, partitions);
        logger.Debug("Assigned partitions.",
            partitions.Select(p => new { Topic = p.Topic, Partition = p.Partition.Value }));
    }

    public void HandlePartitionRevoked(string connector, int taskId, IList<TopicPartitionOffset> offsets)
    {
        if (!(offsets?.Any() ?? false))
        {
            logger.Trace( "No partitions revoked.");
            return;
        }

        executionContext.RevokePartitions(connector, taskId, offsets.Select(o => o.TopicPartition).ToList());
        logger.Debug("Revoked partitions.",
            offsets.Select(p => new { p.Topic, Partition = p.Partition.Value, Offset = p.Offset.Value }));
    }

    public void HandleOffsetCommitted(CommittedOffsets offsets)
    {
        if (offsets.Error?.Code != ErrorCode.NoError)
        {
            logger.Warning("Error committing offsets.",
                offsets.Offsets?.Select(o => new
                {
                    o.Topic, Partition = o.Partition.Value, offset = o.Offset.Value, o.Error.Reason,
                    Status = o.Error.Code
                }));
            return;
        }

        if (!(offsets.Offsets?.Any() ?? false))
        {
            logger.Trace("No offsets committed.");
            return;
        }

        logger.Debug("Offsets committed.",
            offsets.Offsets?.Select(o => new
                { o.Topic, Partition = o.Partition.Value, Offset = o.Offset.Value, Status = o.Error.Code }));

    }
}
