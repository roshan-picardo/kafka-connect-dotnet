using System;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Builders;
using Kafka.Connect.Connectors;
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

        [OperationLog("Subscribing to the topics.")]
        public IConsumer<byte[], byte[]> Subscribe(string connector, int taskId)
        {
            var topics = _configurationProvider.GetTopics(connector);
            if (!(topics?.Any(t => !string.IsNullOrWhiteSpace(t)) ?? false)) return null;
            try
            {
                var consumer = _kafkaClientBuilder.GetConsumer(connector, taskId);
                // TODO: Should we time this as well??
                consumer.Subscribe(topics);
                return consumer;
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed to establish the connection Kafka brokers.");
                // throw?? 
            }
            return null;
        }


        [OperationLog("Consuming messages.")]
        public async Task<SinkRecordBatch> Consume(IConsumer<byte[], byte[]> consumer, string connector, int taskId)
        {
            var retryConfig = _configurationProvider.GetRetriesConfig(connector);
            var batch = await _retriableHandler.Retry(
                async () => await ConsumeInternal(consumer, connector, taskId),
                retryConfig.Attempts, retryConfig.DelayTimeoutMs);
            if (batch == null || batch.IsEmpty) return batch;
            foreach (var record in batch)
            {
                record.Consumed.Message.Headers ??= new Headers();
                record.Consumed.Message.Headers.StartTiming(record.Consumed.Message.Timestamp.UnixTimestampMs);
            }
            return batch;
        }

        private async Task<SinkRecordBatch> ConsumeInternal(IConsumer<byte[], byte[]> consumer, string connector, int taskId)
        {
            var batch = new SinkRecordBatch("internal");
            var batchPollContext = _executionContext.GetOrSetBatchContext(connector, taskId);
            try
            {
                var maxBatchSize = _configurationProvider.GetBatchConfig(connector).Size;
                _logger.LogTrace("{@poll}",
                    new
                    {
                        poll = batchPollContext.Iteration, status = SinkStatus.Polling, 
                    });
                do
                {
                    //TODO: Should we time this?
                    var consumed = await Task.Run(() => consumer.Consume(batchPollContext.Token));
                    batchPollContext.StartTiming();
                    if (consumed == null)
                    {
                        //unlikely that we reach here when we using Consume(cts.Token).
                        continue;
                    }

                    _logger.LogDebug("{@Log}", new
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
                    _logger.LogWarning(ex, "{@Log}",
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