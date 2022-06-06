using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Builders;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;
using Kafka.Connect.Utilities;
using Microsoft.Extensions.Logging;

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

        [OperationLog("Validate and subscribe to the topics.")]
        public IConsumer<byte[], byte[]> Subscribe(string connector, int taskId)
        {
            [OperationLog("Subscribing to the topics.")]
            void SubscribeInternal(IConsumer<byte[], byte[]> consumer, IEnumerable<string> topics) => consumer.Subscribe(topics);
            
            var topics = _configurationProvider.GetTopics(connector);
            if (!(topics?.Any(t => !string.IsNullOrWhiteSpace(t)) ?? false))
            {
                _logger.LogWarning(Constants.AtLog, new {Message="No topics to subscribe."});
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
                    _logger.LogCritical(ex, Constants.AtLog,new { Message = "Failed to establish the connection Kafka brokers."});
                }
            }

            return null;
        }


        [OperationLog("Consume and batch messages.")]
        public async Task<SinkRecordBatch> Consume(IConsumer<byte[], byte[]> consumer, string connector, int taskId)
        {
            var batch = await _retriableHandler.Retry(() => ConsumeInternal(consumer, connector, taskId), connector);
            if (batch == null || batch.IsEmpty)
            {
                _logger.LogDebug(Constants.AtLog, new {Message="There aren't any messages in the batch to process."});
                return batch;
            }
            foreach (var record in batch)
            {
                record.Consumed.Message.Headers ??= new Headers();
                record.Consumed.Message.Headers.StartTiming(record.Consumed.Message.Timestamp.UnixTimestampMs);
            }
            return batch;
        }

        private async Task<SinkRecordBatch> ConsumeInternal(IConsumer<byte[], byte[]> consumer, string connector, int taskId)
        {
            [OperationLog("Consuming message.")]
            ConsumeResult<byte[], byte[]> Consuming(IConsumer<byte[], byte[]> consumerInternal, CancellationToken token) => consumerInternal.Consume(token);
            
            var batch = new SinkRecordBatch("internal");
            var batchPollContext = _executionContext.GetOrSetBatchContext(connector, taskId);
            try
            {
                var maxBatchSize = _configurationProvider.GetBatchConfig(connector).Size;
                _logger.LogTrace(Constants.AtLog,
                    new
                    {
                        Message = $"Polling for messages. #{batchPollContext.Iteration:00000}"
                    });
                do
                {
                    var consumed = await Task.Run(() => Consuming(consumer, batchPollContext.Token));
                    batchPollContext.StartTiming();
                    if (consumed == null)
                    {
                        //unlikely that we reach here when we using Consume(cts.Token).
                        continue;
                    }

                    _logger.LogDebug(Constants.AtLog, new
                    {
                        Message = "Message consumed.",
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
                    _logger.LogWarning(ex, Constants.AtLog,
                        new {Message = "Consume failed. Part of the batch will be processed.", batch.Count});
                }
                else
                {
                    if (ex is OperationCanceledException)
                    {
                        _logger.LogTrace("{@Log}",new {Message = "Task has been cancelled. The consume operation will be terminated."});
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