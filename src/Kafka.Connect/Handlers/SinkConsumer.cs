using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Builders;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;
using Kafka.Connect.Utilities;

namespace Kafka.Connect.Handlers
{
    public class SinkConsumer : ISinkConsumer
    {
        private readonly ILogger<SinkConsumer> _logger;
        private readonly IExecutionContext _executionContext;
        private readonly IRetriableHandler _retriableHandler;
        private readonly IConfigurationProvider _configurationProvider;
        private readonly IKafkaClientBuilder _kafkaClientBuilder;

        public SinkConsumer(ILogger<SinkConsumer> logger, IExecutionContext executionContext,
            IRetriableHandler retriableHandler, IConfigurationProvider configurationProvider, IKafkaClientBuilder kafkaClientBuilder)
        {
            _logger = logger;
            _executionContext = executionContext;
            _retriableHandler = retriableHandler;
            _configurationProvider = configurationProvider;
            _kafkaClientBuilder = kafkaClientBuilder;
        }

        public IConsumer<byte[], byte[]> Subscribe(string connector, int taskId)
        {
            using (_logger.Track("Validate and subscribe to the topics."))
            {
                void SubscribeInternal(IConsumer<byte[], byte[]> consumer, IEnumerable<string> topics)
                {
                    using (_logger.Track("Subscribing to the topics."))
                    {
                        consumer.Subscribe(topics);
                    }
                }

                var topics = _configurationProvider.GetTopics(connector);
                if (!(topics?.Any(t => !string.IsNullOrWhiteSpace(t)) ?? false))
                {
                    _logger.Warning("No topics to subscribe.");
                }
                else
                {
                    try
                    {
                        var consumer = _kafkaClientBuilder.GetConsumer(connector, taskId);
                        SubscribeInternal(consumer, topics);
                        return consumer;
                    }
                    catch (Exception ex)
                    {
                        _logger.Critical("Failed to establish the connection Kafka brokers.", ex);
                    }
                }
                return null;
            }
        }


        public async Task<SinkRecordBatch> Consume(IConsumer<byte[], byte[]> consumer, string connector, int taskId)
        {
            using (_logger.Track("Consume and batch messages."))
            {
                var batch = await _retriableHandler.Retry(() => ConsumeInternal(consumer, connector, taskId),
                    connector);
                if (batch == null || batch.IsEmpty)
                {
                    _logger.Debug("There aren't any messages in the batch to process.");
                    return batch;
                }

                foreach (var record in batch)
                {
                    record.Consumed.Message.Headers ??= new Headers();
                    record.Consumed.Message.Headers.StartTiming(record.Consumed.Message.Timestamp.UnixTimestampMs);
                }

                return batch;
            }
        }

        private async Task<SinkRecordBatch> ConsumeInternal(IConsumer<byte[], byte[]> consumer, string connector, int taskId)
        {
            ConsumeResult<byte[], byte[]> Consuming(IConsumer<byte[], byte[]> consumerInternal, CancellationToken token)
            {
                using (_logger.Track("Consuming message."))
                {
                    return consumerInternal.Consume(token);
                }
            }
            
            var batch = new SinkRecordBatch("internal");
            var batchPollContext = _executionContext.GetOrSetBatchContext(connector, taskId);
            try
            {
                var maxBatchSize = _configurationProvider.GetBatchConfig(connector).Size;
                _logger.Trace("Polling for messages.", new { batchPollContext.Iteration });
                do
                {
                    var consumed = await Task.Run(() => Consuming(consumer, batchPollContext.Token));
                    batchPollContext.StartTiming();
                    if (consumed == null)
                    {
                        //unlikely that we reach here when we using Consume(cts.Token).
                        continue;
                    }

                    _logger.Debug("Message consumed.", new
                    {
                        consumed.Topic,
                        Partition = consumed.Partition.Value,
                        Offset = consumed.TopicPartitionOffset.Offset.Value,
                        consumed.IsPartitionEOF,
                        Timestamp = consumed.Message?.Timestamp.UtcDateTime
                    });
                    
                    if (consumed.IsPartitionEOF)
                    {
                        batch.SetPartitionEof(consumed.TopicPartitionOffset);
                        break;
                    }
                    batch.Add(consumed);

                } while (--maxBatchSize > 0);
            }
            catch (Exception ex)
            {
                if (!batch.IsEmpty)
                {
                    //if batch already got a few records lets process them before failing.
                    _logger.Warning("Consume failed. Part of the batch will be processed.", new {batch.Count}, ex);
                }
                else
                {
                    if (ex is OperationCanceledException)
                    {
                        _logger.Trace( "Task has been cancelled. The consume operation will be terminated.", ex);
                    }
                    else
                    {
                        if (ex is ConsumeException ce)
                        {
                            throw new ConnectRetriableException(ce.Error, ce.InnerException);
                        }

                        throw new ConnectDataException(ErrorCode.Local_Fatal, ex);
                    }
                }
            }

            return batch;
        }

    }
}