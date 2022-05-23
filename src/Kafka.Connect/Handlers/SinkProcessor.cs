using System;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;
using Kafka.Connect.Serializers;
using Kafka.Connect.Utilities;
using Microsoft.Extensions.Logging;
using Serilog.Context;
using Serilog.Core.Enrichers;

namespace Kafka.Connect.Handlers
{
    public class SinkProcessor : ISinkProcessor
    {
        private readonly ILogger<SinkProcessor> _logger;
        private readonly IMessageConverter _messageConverter;
        private readonly IMessageHandler _messageHandler;
        private readonly ISinkHandlerProvider _sinkHandlerProvider;
        private readonly IConfigurationProvider _configurationProvider;

        public SinkProcessor(ILogger<SinkProcessor> logger, IMessageConverter messageConverter,
            IMessageHandler messageHandler, ISinkHandlerProvider sinkHandlerProvider, IConfigurationProvider configurationProvider)
        {
            _logger = logger;
            _messageConverter = messageConverter;
            _messageHandler = messageHandler;
            _sinkHandlerProvider = sinkHandlerProvider;
            _configurationProvider = configurationProvider;
        }

        [OperationLog("Processing the batch.")]
        public async Task Process(SinkRecordBatch batch, string connector)
        {
            foreach (var topicBatch in batch.BatchByTopicPartition)
            {
                using (LogContext.Push(new PropertyEnricher("Topic", topicBatch.Topic),
                    new PropertyEnricher("Partition", topicBatch.Partition)))
                {
                    await topicBatch.Batch.ForEachAsync(async record =>
                    {
                        using (LogContext.PushProperty("Offset", record.Offset))
                        {
                            record.Status = SinkStatus.Processing;
                            var (keyToken, valueToken) = await _messageConverter.Deserialize(record.Consumed, connector);
                            record.Parsed(keyToken, valueToken);
                            record.LogDocument();
                            (record.Skip, record.Data) = await _messageHandler.Process(record, connector);
                            record.Status = SinkStatus.Processed;
                        }
                    }, (record, exception) => exception.SetLogContext(record), _configurationProvider.GetBatchConfig(connector).Parallelism);
                }
            }
        }

        [OperationLog("Sinking the batch.")]
        public async Task Sink(SinkRecordBatch batch, string connector)
        {
            var config = _configurationProvider.GetSinkConfig(connector);
            var sinkHandler = _sinkHandlerProvider.GetSinkHandler(connector);
            if (sinkHandler == null)
            {
                _logger.LogWarning("{@Log}",
                    new
                    {
                        Message = "Sink handler is not specified. Check if the handler is configured properly, and restart the connector.",
                    });
                batch.SkipAll();
                return;
            }
            await sinkHandler.Put(batch);
        }
    }
}
