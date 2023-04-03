using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Builders;
using Kafka.Connect.Configurations;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;
using Serilog.Context;
using Serilog.Core.Enrichers;

namespace Kafka.Connect.Handlers
{
    public class PartitionHandler : IPartitionHandler
    {
        private readonly ILogger<PartitionHandler> _logger;
        private readonly IKafkaClientBuilder _kafkaClientBuilder;
        private readonly IConfigurationProvider _configurationProvider;

        public PartitionHandler(ILogger<PartitionHandler> logger, IKafkaClientBuilder kafkaClientBuilder, IConfigurationProvider configurationProvider)
        {
            _logger = logger;
            _kafkaClientBuilder = kafkaClientBuilder;
            _configurationProvider = configurationProvider;
        }

        public void CommitOffsets(SinkRecordBatch batch, IConsumer<byte[], byte[]> consumer)
        {
            using (_logger.Track("Committing offsets."))
            {
                var offsets = batch.GetCommitReadyOffsets().ToList();
                if (!offsets.Any())
                {
                    return;
                }

                var (enableAutoCommit, enableAutoOffsetStore) = _configurationProvider.GetAutoCommitConfig();

                var maxOffsets = GetMaxOffsets(offsets);

                if (!enableAutoCommit)
                {
                    consumer.Commit(maxOffsets);
                }
                else if (!enableAutoOffsetStore)
                {
                    foreach (var commitOffset in maxOffsets)
                    {
                        consumer.StoreOffset(commitOffset);
                    }
                }
            }
        }

        public async Task NotifyEndOfPartition(SinkRecordBatch batch, string connector, int taskId)
        {
            using (_logger.Track("Notify end of the partition."))
            {
                Task<DeliveryResult<byte[], byte[]>> Produce(IProducer<byte[], byte[]> producer, string topic,
                    Message<byte[], byte[]> message)
                {
                    using (_logger.Track("Producing EOF notification message."))
                    {
                        return producer.ProduceAsync(topic, message);
                    }
                }

                var eofSignal = _configurationProvider.GetEofSignalConfig(connector) ?? new EofConfig();
                if (eofSignal.Enabled && !string.IsNullOrWhiteSpace(eofSignal.Topic))
                {
                    var eofPartitions = batch.GetEofPartitions().ToList();
                    if (!eofPartitions.Any())
                    {
                        return;
                    }

                    foreach (var commitReadyOffset in GetMaxOffsets(batch.GetCommitReadyOffsets()))
                    {
                        var eofPartition = eofPartitions.SingleOrDefault(o =>
                            o.Topic == commitReadyOffset.Topic &&
                            o.Partition.Value == commitReadyOffset.Partition.Value &&
                            o.Offset.Value == commitReadyOffset.Offset.Value);
                        if (eofPartition == null) continue;
                        using (LogContext.Push(new PropertyEnricher("Topic", eofPartition.Topic),
                                   new PropertyEnricher("Partition", eofPartition.Partition)))
                        {
                            using var producer = _kafkaClientBuilder.GetProducer(connector);
                            {
                                if (producer == null)
                                {
                                    _logger.Warning("No producer configured to publish EOF message.");
                                    continue;
                                }

                                var message = new Message<byte[], byte[]>
                                {
                                    Key = ByteConvert.Serialize(Guid.NewGuid()),
                                    Value = ByteConvert.Serialize(new EndOfPartitionMessage
                                    {
                                        Connector = connector,
                                        TaskId = taskId,
                                        Topic = eofPartition.Topic,
                                        Partition = eofPartition.Partition.Value,
                                        Offset = eofPartition.Offset.Value
                                    })
                                };

                                var delivered = await Produce(producer, eofSignal.Topic, message);
                                _logger.Info("EOF message delivered.", new
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
        }


        private static IEnumerable<TopicPartitionOffset> GetMaxOffsets(IEnumerable<TopicPartitionOffset> offsets)
        {
            var maxOffsets = offsets.GroupBy(g => new {g.Topic, g.Partition.Value},
                (_, r) =>
                {
                    var offset = r.ToList();
                    return
                        offset.SingleOrDefault(s =>
                            s.Offset.Value == offset.Max(o => o.Offset.Value));
                }).ToList();
            foreach (var commitOffset in maxOffsets.Select(offset =>
                new TopicPartitionOffset(offset.TopicPartition, new Offset(offset.Offset + 1))))
            {
                yield return commitOffset;
            }
        }
    }
}