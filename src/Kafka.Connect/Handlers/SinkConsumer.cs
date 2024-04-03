using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Builders;
using Kafka.Connect.Connectors;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Providers;

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

        public async Task<IList<SinkRecord>> Consume(IConsumer<byte[], byte[]> consumer, string connector, int taskId, bool consumeAll = false)
        {
            using (_logger.Track("Consume and batch messages."))
            {
                var batch =  await ConsumeInternal(consumer, connector, taskId, consumeAll);
                if (!batch.Any())
                {
                    _logger.Debug("There aren't any messages in the batch to process.");
                }
                return batch;
            }
        }

        public void Commit(IConsumer<byte[], byte[]> consumer, CommandRecord sourceCommand)
        {
            consumer.Commit(new[] { new TopicPartitionOffset(sourceCommand.Topic, sourceCommand.Partition, sourceCommand.Offset + 1) });
        }

        private async Task<IList<SinkRecord>> ConsumeInternal(IConsumer<byte[], byte[]> consumer, string connector, int taskId, bool consumeAll)
        {
            var batch = new List<SinkRecord>();
            var batchPollContext = _executionContext.GetOrSetBatchContext(connector, taskId);
            try
            {
                var maxBatchSize = _configurationProvider.GetBatchConfig(connector).Size;
                _logger.Trace("Polling for messages.", new { batchPollContext.Iteration });
                do
                {
                    var consumed =  await Task.Run(() => consumer.Consume(batchPollContext.Token));
                    batchPollContext.StartTiming();
                    if (consumed == null)
                    {
                        //unlikely that we reach here when we using Consume(cts.Token).
                        continue;
                    }
                    _executionContext.SetPartitionEof(connector, taskId, consumed.Topic, consumed.Partition, false);

                    _logger.Debug("Message consumed.", new
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
                    _executionContext.SetPartitionEof(connector, taskId, consumed.Topic, consumed.Partition, true);
                    if (_executionContext.AllPartitionEof(connector, taskId))
                    {
                        break;
                    }
                } while (consumeAll || --maxBatchSize > 0);
            }
            catch (Exception ex)
            {
                if (batch.Any())
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
                            throw new ConnectRetriableException(ce.Error.Reason, ce.InnerException);
                        }

                        throw new ConnectDataException(ErrorCode.Local_Fatal.GetReason(), ex);
                    }
                }
            }

            return batch;
        }

    }
}