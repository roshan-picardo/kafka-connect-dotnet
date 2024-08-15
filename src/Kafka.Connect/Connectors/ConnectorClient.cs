using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Builders;
using Kafka.Connect.Configurations;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;
using Kafka.Connect.Utilities;

namespace Kafka.Connect.Connectors;

public interface IConnectorClient
{
    bool TryBuildSubscriber(string connector, int taskId);
    bool TryBuildPublisher(string connector);
    Task<IList<SinkRecord>> Consume(string connector, int taskId, CancellationToken token);
    Task Produce(string connector, List<IConnectRecord> records);
    Task Produce(TopicPartition topicPartition, Message<byte[], byte[]> message);
    void Commit(IList<(string Topic, int Partition, long Offset)> offsets);

    Task NotifyEndOfPartition(
        string connector,
        int taskId,
        IList<(string Topic, int Partition, long Offset)> eofPartitions,
        IList<(string Topic, int Partition, long Offset)> commitReadyOffsets);

    void Close();
}

public class ConnectorClient(
    IKafkaClientBuilder kafkaClientBuilder,
    IConfigurationProvider configurationProvider,
    IExecutionContext executionContext,
    ILogger<ConnectorClient> logger) : IConnectorClient
{
    private IConsumer<byte[], byte[]> _consumer;
    private IProducer<byte[], byte[]> _producer;

    public bool TryBuildSubscriber(string connector, int taskId)
    {
        using (logger.Track("Validate and subscribe to the topics."))
        {
            var topics = configurationProvider.GetTopics(connector);
            if (!(topics?.Any(t => !string.IsNullOrWhiteSpace(t)) ?? false))
            {
                logger.Warning("No topics to subscribe.");
            }
            else
            {
                try
                {
                    _consumer = kafkaClientBuilder.GetConsumer(connector, taskId);
                    _consumer.Subscribe(topics);
                    return true;
                }
                catch (Exception ex)
                {
                    logger.Critical("Failed to establish the connection Kafka brokers.", ex);
                }
            }

            return false;
        }
    }

    public bool TryBuildPublisher(string connector)
    {
        _producer = kafkaClientBuilder.GetProducer(connector);
        if (_producer != null)
        {
            return true;
        }

        logger.Warning("Failed to create the publisher, exiting from the source task.");
        return false;
    }

    public async Task<IList<SinkRecord>> Consume(string connector, int taskId, CancellationToken token)
    {
        using (logger.Track("Consume and batch messages."))
        {
            var batch = await ConsumeInternal(connector, taskId, token);
            if (!batch.Any())
            {
                logger.Debug("There aren't any messages in the batch to process.");
            }

            return batch;
        }
    }

    public async Task Produce(string connector, List<IConnectRecord> records)
    {
        using (logger.Track("Publishing the batch."))
        {
            var parallelOptions = configurationProvider.GetParallelRetryOptions(connector);
            await records.ForEachAsync(parallelOptions,
                async cr =>
                {
                    if (cr is SourceRecord { Publishing: true } record)
                    {
                        using (ConnectLog.TopicPartitionOffset(record.Topic))
                        {
                            record.Status = Status.Publishing; 
                            //TODO: handle record.skip a bit differently
                            var delivered = await _producer.ProduceAsync(record.Topic,
                                new Message<byte[], byte[]>
                                {
                                    Key = record.Serialized.Key,
                                    Value = record.Serialized.Value,
                                    Headers = record.Serialized.Headers?.ToMessageHeaders()
                                });
                            record.Published(delivered.Topic, delivered.Partition, delivered.Offset);
                        }
                    }
                });
        }
    }

    public Task Produce(TopicPartition topicPartition, Message<byte[], byte[]> message) =>
        _producer.ProduceAsync(topicPartition, message);

    public void Commit(IList<(string Topic, int Partition, long Offset)> offsets)
    {
        using (logger.Track("Committing offsets."))
        {
            if (!offsets.Any())
            {
                return;
            }

            var (enableAutoCommit, enableAutoOffsetStore) = configurationProvider.GetAutoCommitConfig();

            var maxOffsets = GetMaxOffsets(offsets);

            if (!enableAutoCommit)
            {
                _consumer.Commit(maxOffsets);
            }
            else if (!enableAutoOffsetStore)
            {
                maxOffsets.ForEach(_consumer.StoreOffset);
            }
        }
    }

    public async Task NotifyEndOfPartition(
        string connector,
        int taskId,
        IList<(string Topic, int Partition, long Offset)> eofPartitions,
        IList<(string Topic, int Partition, long Offset)> commitReadyOffsets)
    {
        using (logger.Track("Notify end of the partition."))
        {
            var eofSignal = configurationProvider.GetEofSignalConfig(connector) ?? new EofConfig();
            if (eofSignal.Enabled && !string.IsNullOrWhiteSpace(eofSignal.Topic))
            {
                if (!eofPartitions.Any())
                {
                    return;
                }

                foreach (var commitReadyOffset in GetMaxOffsets(commitReadyOffsets))
                {
                    var eofPartition = eofPartitions.SingleOrDefault(o =>
                        o.Topic == commitReadyOffset.Topic &&
                        o.Partition == commitReadyOffset.Partition.Value &&
                        o.Offset == commitReadyOffset.Offset.Value);
                    if (eofPartition == default) continue;
                    using (ConnectLog.TopicPartitionOffset(eofPartition.Topic, eofPartition.Partition,
                               eofPartition.Offset))
                    {
                        var message = new Message<byte[], byte[]>
                        {
                            Key = ByteConvert.Serialize(Guid.NewGuid()),
                            Value = ByteConvert.Serialize(new EndOfPartitionMessage
                            {
                                Connector = connector,
                                TaskId = taskId,
                                Topic = eofPartition.Topic,
                                Partition = eofPartition.Partition,
                                Offset = eofPartition.Offset
                            })
                        };

                        var delivered = await _producer.ProduceAsync(eofSignal.Topic, message);
                        logger.Info("EOF message delivered.", new
                        {
                            delivered.Topic,
                            Partition = delivered.Partition.Value,
                            Offset = delivered.Offset.Value
                        });
                    }
                }
            }
        }
    }

    public void Close()
    {
        _consumer?.Close();
        _consumer?.Dispose();
        _producer?.Dispose();
    }
    
    private async Task<IList<SinkRecord>> ConsumeInternal(string connector, int taskId, CancellationToken token)
    {
        var internalTopics = configurationProvider.GetTopics();
        var consumeAll = _consumer.Subscription.All(s => s == internalTopics.Command);
        var batch = new List<SinkRecord>();
        try
        {
            var maxBatchSize = configurationProvider.GetBatchConfig(connector).Size;
            do
            {
                var consumed = await Task.Run(() => _consumer.Consume(token), token);
                if (consumed == null)
                {
                    //unlikely that we reach here when we using Consume(cts.Token).
                    continue;
                }

                executionContext.SetPartitionEof(connector, taskId, consumed.Topic, consumed.Partition, false);

                logger.Debug("Message consumed.", new
                {
                    consumed.Topic,
                    Partition = consumed.Partition.Value,
                    Offset = consumed.TopicPartitionOffset.Offset.Value,
                    consumed.IsPartitionEOF,
                    Timestamp = consumed.Message?.Timestamp.UtcDateTime
                });

                batch.Add(new SinkRecord(consumed));

                if (!consumed.IsPartitionEOF)
                {
                    continue;
                }

                //batch.SetPartitionEof(consumed.Topic, consumed.Partition.Value, consumed.Offset.Value);
                executionContext.SetPartitionEof(connector, taskId, consumed.Topic, consumed.Partition, true);
                if (executionContext.AllPartitionEof(connector, taskId))
                {
                    break;
                }
            } while (consumeAll || --maxBatchSize > 0);
        }
        catch (Exception ex)
        {
            if (batch.Count > 0)
            {
                //if batch already got a few records lets process them before failing.
                logger.Warning("Consume failed. Part of the batch will be processed.", new { batch.Count }, ex);
            }
            else
            {
                if (ex is OperationCanceledException)
                {
                    logger.Trace("Task has been cancelled. The consume operation will be terminated.", ex);
                }
                else
                {
                    if (ex is ConsumeException ce)
                    {
                        throw new ConnectRetriableException(ce.Error.Reason, ce.InnerException);
                    }

                    throw new ConnectDataException(ErrorCode.Local_Fatal.GetReason(), ex);
                }
            }
        }

        return batch;
    }

    private static IEnumerable<TopicPartitionOffset> GetMaxOffsets(
        IEnumerable<(string Topic, int Partition, long Offset)> offsets)
    {
        var maxOffsets = offsets.GroupBy(g => new { g.Topic, g.Partition },
            (_, r) =>
            {
                var offset = r.ToList();
                return
                    offset.SingleOrDefault(s =>
                        s.Offset == offset.Max(o => o.Offset));
            }).ToList();
        foreach (var commitOffset in maxOffsets.Select(offset =>
                     new TopicPartitionOffset(offset.Topic, new Partition(offset.Partition),
                         new Offset(offset.Offset + 1))))
        {
            yield return commitOffset;
        }
    }
}
