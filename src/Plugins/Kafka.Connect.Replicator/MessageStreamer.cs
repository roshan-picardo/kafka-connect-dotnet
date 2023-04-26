using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Builders;
using Kafka.Connect.Config;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;
using Kafka.Connect.Serializers;
using Kafka.Connect.Utilities;
using Microsoft.Extensions.Logging;
using Serilog.Context;
using Serilog.Core.Enrichers;

namespace Kafka.Connect.Handlers
{
    public class MessageStreamer : IMessageStreamer
    {
        private readonly ILogger<MessageStreamer> _logger;
        private readonly IProcessorServiceProvider _processorServiceProvider;
        private readonly IMessageConverter _messageConverter;
        private readonly IKafkaClientBuilder _kafkaClientBuilder;

        public MessageStreamer(ILogger<MessageStreamer> logger, IProcessorServiceProvider processorServiceProvider,
            IMessageConverter messageConverter, IKafkaClientBuilder kafkaClientBuilder)
        {
            _logger = logger;
            _processorServiceProvider = processorServiceProvider;
            _messageConverter = messageConverter;
            _kafkaClientBuilder = kafkaClientBuilder;
        }

        public async Task<SinkRecordBatch> Enrich(SinkRecordBatch batch, ConnectorConfig config)
        {
            if (config?.Publisher == null || !config.Publisher.Enabled)
            {
                return batch;
            }

            if (!config.Publisher.Topics?.Any(t => !string.IsNullOrWhiteSpace(t.Name)) ?? true)
            {
                _logger.LogWarning("{@publish}",
                    new {message = "No destination topics configured. Skipping batch from publishing."});
                batch.ExcludeAll();
                return batch;
            }

            var batchWatch = Stopwatch.StartNew();
            var parallelism = config.GetBatchParallelism();
            var retry = config.GetRetriesConfig();
            foreach (var topicBatch in batch.BatchByTopicPartition)
            {
                using (LogContext.Push(new PropertyEnricher("topic", topicBatch.Topic),
                    new PropertyEnricher("partition", topicBatch.Partition)))
                {
                    await topicBatch.Batch.ForEachAsync(async record =>
                        {
                            using (LogContext.PushProperty("offset", record.Offset))
                            {
                                if (retry.Continue && record.IsEnriched)
                                {
                                    _logger.LogDebug("{@debug}",
                                        new {message = "Message already enriched."});
                                    return;
                                }
                                var recordWatch = Stopwatch.StartNew();
                                record.Status = SinkStatus.Excluding;
                                var processorConfig = config.Publisher.Enrichers?.FirstOrDefault(t =>
                                    string.IsNullOrWhiteSpace(t.Topic) || t.Topic == record.Topic);
                                if (processorConfig == null)
                                {
                                    _logger.LogTrace("{@debug}",
                                        new
                                        {
                                            message =
                                                $"Post Processor is not configured for this topic. {record.Topic}"
                                        });
                                    record.UpdateStatus();
                                    return;
                                }

                                var enrichers = _processorServiceProvider.GetEnrichers()?.ToList();
                                var enricher =
                                    enrichers?.SingleOrDefault(p => p.IsOfType(processorConfig.Name));
                                if (enricher == null)
                                {
                                    _logger.LogTrace("{@debug}",
                                        new {message = $"Processor is not registered. {processorConfig.Name}"});
                                    record.UpdateStatus();
                                    return;
                                }

                                // message enrichment starts here
                                record.Status = SinkStatus.Enriching;
                                var processed = await enricher.Apply(record, processorConfig.Properties);

                                foreach (var (topic, message) in processed)
                                {
                                    record.PublishMessages ??= new Dictionary<string, Message<byte[], byte[]>>();
                                    record.PublishMessages.Add(topic, await _messageConverter.Serialize(
                                        config.Publisher.Serializers,
                                        config.Publisher.Topics.SingleOrDefault(t => t.Name == topic),
                                        message.Key, message.Value));
                                }

                                record.UpdateStatus();

                                _logger.LogTrace("{@timer}",
                                    new
                                    {
                                        duration = recordWatch.EndTiming(),
                                        message = "Message enriched and serialized successfully."
                                    });
                            }
                        },
                        (record, exception) => exception.SetLogContext(record), parallelism);
                }
            }

            _logger.LogTrace("{@timer}",
                new
                {
                    duration = batchWatch.EndTiming(),
                    message = "Batch enriched and serialized successfully."
                });
            return batch;
        }

        public async Task<SinkRecordBatch> Publish(SinkRecordBatch batch, ConnectorConfig config)
        {
            if (config?.Publisher == null || !config.Publisher.Enabled)
            {
                return batch;
            }

            using var producer = _kafkaClientBuilder.GetProducer(config.Publisher);
            if (producer == null)
            {
                _logger.LogWarning("{@publish}",
                    new {message = "Unable to build the producer, for the incoming message."});
                throw new ConnectDataException(ErrorCode.Local_Application,
                    new ArgumentException("Unable to create a Kafka producer."));
            }

            var batchWatch = Stopwatch.StartNew();
            var parallelism = config.GetBatchParallelism();
            try
            {
                foreach (var topicBatch in batch.BatchByTopicPartition)
                {
                    using (LogContext.Push(new PropertyEnricher("topic", topicBatch.Topic),
                        new PropertyEnricher("partition", topicBatch.Partition)))
                    {
                        await topicBatch.Batch.ForEachAsync(async record =>
                        {
                            var recordWatch = Stopwatch.StartNew();
                            using (LogContext.PushProperty("offset", record.Offset))
                            {
                                if (record.IsPublished)
                                {
                                    _logger.LogDebug("{@debug}", new {message = "Message already published."});
                                    return;
                                }
                                if (record.CanPublish)
                                {
                                    record.Status = SinkStatus.Publishing;
                                    var delivered = new List<DeliveryResult<byte[], byte[]>>();
                                    foreach (var (topic, message) in record.PublishMessages)
                                    {
                                        delivered.Add(await producer.ProduceAsync(topic, message));
                                    }

                                    record.AddLog("published",
                                        (from d in delivered
                                            select new
                                            {
                                                topic = d.Topic,
                                                partition = d.Partition.Value,
                                                offset = d.Offset.Value,
                                                status = d.Status
                                            }).ToList);
                                    record.UpdateStatus();
                                }

                                _logger.LogTrace("{@timer}",
                                    new
                                    {
                                        duration = recordWatch.EndTiming(),
                                        message = "Message published successfully."
                                    });
                            }
                        }, (record, exception) => exception.SetLogContext(record), parallelism);
                    }
                }
            }
            finally
            {
                producer.Flush();
                _logger.LogTrace("{@timer}",
                    new
                    {
                        duration = batchWatch.EndTiming(),
                        message = "Messages published successfully."
                    });
            }
            return batch;
        }
    }
}