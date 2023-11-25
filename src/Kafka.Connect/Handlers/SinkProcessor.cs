using System;
using System.Collections.Generic;
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

namespace Kafka.Connect.Handlers;

public class SinkProcessor : ISinkProcessor
{
    private readonly ILogger<SinkProcessor> _logger;
    private readonly IMessageConverter _messageConverter;
    private readonly IMessageHandler _messageHandler;
    private readonly ISinkHandlerProvider _sinkHandlerProvider;
    private readonly IConfigurationProvider _configurationProvider;

    public SinkProcessor(
        ILogger<SinkProcessor> logger,
        IMessageConverter messageConverter,
        IMessageHandler messageHandler,
        ISinkHandlerProvider sinkHandlerProvider,
        IConfigurationProvider configurationProvider)
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
            foreach (var topicBatch in batch.GetByTopicPartition<Models.SinkRecord>())
            {
                using (LogContext.Push(new PropertyEnricher(Constants.Topic, topicBatch.Topic),
                           new PropertyEnricher(Constants.Partition, topicBatch.Partition)))
                {
                    await topicBatch.Batch.ForEachAsync(async record =>
                        {
                            using (LogContext.PushProperty(Constants.Offset, record.Offset))
                            {
                                record.Status = SinkStatus.Processing;
                                record.Deserialized = await _messageConverter.Deserialize(connector, record.Topic, record.Serialized);
                                _logger.Document(record.Deserialized);
                                record.Deserialized = await _messageHandler.Process(connector, record.Topic,
                                    new ConnectMessage<IDictionary<string, object>>
                                    {
                                        Skip = false,
                                        Key = record.Deserialized.Key?.ToDictionary(),
                                        Value = record.Deserialized.Value?.ToDictionary()
                                    });
                                record.Skip = record.Deserialized.Skip;
                                record.Status = SinkStatus.Processed;
                            }
                        }, (record, exception) => exception.SetLogContext(record),
                        _configurationProvider.GetBatchConfig(connector).Parallelism);
                }
            }
        }
    }

    public Task<T> Process<T>(Models.SinkRecord record, string connector)
    {
        using (_logger.Track("Processing record"))
        {
            throw new NotImplementedException();
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