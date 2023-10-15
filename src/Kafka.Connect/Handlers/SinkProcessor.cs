using System;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;
using Kafka.Connect.Serializers;
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

        public async Task Process(ConnectRecordBatch batch, string connector)
        {
            using (_logger.Track("Processing the batch."))
            {
                batch ??= new ConnectRecordBatch(connector);
                foreach (var topicBatch in batch.GetByTopicPartition<Models.ConnectRecord>())
                {
                    using (LogContext.Push(new PropertyEnricher(Constants.Topic, topicBatch.Topic),
                               new PropertyEnricher(Constants.Partition, topicBatch.Partition)))
                    {
                        await topicBatch.Batch.ForEachAsync(async record =>
                            {
                                using (LogContext.PushProperty(Constants.Offset, record.Offset))
                                {
                                    record.Status = SinkStatus.Processing;
                                    var (keyToken, valueToken) =
                                        await _messageConverter.Deserialize(record.Topic, record.GetConsumedMessage(), connector);
                                    record.Parsed(keyToken, valueToken);
                                    _logger.Document(record.Message); 
                                    (record.Skip, record.Message) = await _messageHandler.Process(record, connector);
                                    record.Status = SinkStatus.Processed;
                                }
                            }, (record, exception) => exception.SetLogContext(record),
                            _configurationProvider.GetBatchConfig(connector).Parallelism);
                    }
                }
            }
        }

        public async Task<T> Process<T>(Kafka.Connect.Models.ConnectRecord record, string connector)
        {
            using (_logger.Track("Processing record"))
            {
                var (keyToken, valueToken) =
                    await _messageConverter.Deserialize(record.Topic, record.GetConsumedMessage(), connector);
                return valueToken.Value<T>(""); //TODO: fix this
            }
        }

        public async Task Sink(ConnectRecordBatch batch, string connector, int taskId)
        {
            using (_logger.Track("Sinking the batch."))
            {
                if (batch == null || !batch.Any())
                {
                    return;
                }

                var sinkHandler = _sinkHandlerProvider.GetSinkHandler(connector);
                if (sinkHandler == null)
                {
                    _logger.Warning(
                        "Sink handler is not specified. Check if the handler is configured properly, and restart the connector.");
                    batch.SkipAll();
                    return;
                }

                await sinkHandler.Put(batch, connector, _configurationProvider.GetBatchConfig(connector).Parallelism);
            }
        }
    }
}
