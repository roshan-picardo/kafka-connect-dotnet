using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Serializers;
using Newtonsoft.Json.Linq;
using Serilog.Context;
using Serilog.Core.Enrichers;
using IConfigurationProvider = Kafka.Connect.Providers.IConfigurationProvider;

namespace Kafka.Connect.Handlers;

public class SourceProcessor : ISourceProcessor
{
    private readonly ILogger<SourceProcessor> _logger;
    private readonly IMessageConverter _messageConverter;
    private readonly IConfigurationProvider _configurationProvider;
    private readonly IMessageHandler _messageHandler;

    public SourceProcessor(
        ILogger<SourceProcessor> logger, 
        IMessageConverter messageConverter,
        IConfigurationProvider configurationProvider,
        IMessageHandler messageHandler)
    {
        _logger = logger;
        _messageConverter = messageConverter;
        _configurationProvider = configurationProvider;
        _messageHandler = messageHandler;
    }

    public async Task Process(ConnectRecordBatch batch, CommandContext command, string connector)
    {
        using (_logger.Track("Processing the source batch."))
        {
            var sourceConfig = _configurationProvider.GetSourceConfig(connector);
            batch ??= new ConnectRecordBatch(connector);

            await batch.ForEachAsync(async record =>
            {
                record.Status = SinkStatus.Processing;
                await _messageHandler.Process(record, connector);
                await _messageConverter.Serialize(record.Topic, record.DeserializedToken.Key, record.DeserializedToken.Value, connector);
            }, (record, exception) => exception.SetLogContext(batch));
           
        }
    }
    
    public async Task<IList<CommandContext>> Process(ConnectRecordBatch batch, string connector)
    {
        using (_logger.Track("Processing the command batch."))
        {
            var sourceConfig = _configurationProvider.GetSourceConfig(connector);
            var trackBatch = new List<CommandContext>();
            batch ??= new ConnectRecordBatch(connector);
            foreach (var topicBatch in batch.GetByTopicPartition<SinkRecord>())
            {
                using (LogContext.Push(new PropertyEnricher(Constants.Topic, topicBatch.Topic),
                           new PropertyEnricher(Constants.Partition, topicBatch.Partition)))
                {
                    foreach (var record in topicBatch.Batch.OrderByDescending(r=>r.Offset))
                    {
                        using (LogContext.PushProperty(Constants.Offset, record.Offset))
                        {
                            record.DeserializedToken = await _messageConverter.Deserialize(record.Topic, record.Serialized, connector);
                            var context = record.GetValue<CommandContext>();
                            if (context.Command != null && 
                                sourceConfig.Commands.ContainsKey(context.Command.Topic ?? "") && 
                                context.Connector == connector &&
                                !trackBatch.Any(c => c.Connector == connector && c.Command?.Topic == context.Command.Topic))
                            {
                                context.Offset = record.Offset;
                                trackBatch.Add(context);
                            }

                            _logger.Document(record.DeserializedToken);
                        }
                    }
                    /*
                    await topicBatch.Batch.OrderByDescending(r => r.Offset).ForEachAsync(async record =>
                        {
                            using (LogContext.PushProperty(Constants.Offset, record.Offset))
                            {
                                var (keyToken, valueToken) =
                                    await _messageConverter.Deserialize(record.Topic, record.GetConsumedMessage(),
                                        connector);
                                record.Parsed(keyToken, valueToken);
                                var context = record.GetValue<CommandContext>();
                                if (context.Command != null && 
                                    sourceConfig.Commands.ContainsKey(context.Command.Topic ?? "") && 
                                    context.Connector == connector &&
                                    !trackBatch.Any<CommandContext>(c => c.Connector == connector && c.Command?.Topic == context.Command.Topic))
                                {
                                    context.Offset = record.Offset;
                                    trackBatch.Add(context);
                                }

                                _logger.Document(record.Message);
                            }
                        }, (record, exception) => exception.SetLogContext(record), 1);*/
                }
            }

            foreach (var ((topic, command), index) in sourceConfig.Commands.Select((commands, i) => (commands, i)))
            {
                var eof = batch.GetEofPartitions().SingleOrDefault(p => p.Partition == index);
                if (!trackBatch.Exists(c => c.Command.Topic == topic) && !string.IsNullOrWhiteSpace(eof.Topic))
                {
                    trackBatch.Add(new CommandContext
                    {
                        Connector = connector,
                        Partition = index, 
                        Offset = eof.Offset,
                        Command = command,
                        Topic = eof.Topic
                    });
                }
            }

            return trackBatch;
        }
    }

    public Task<Message<byte[], byte[]>> GetCommandMessage(CommandContext context)
    {
        using (_logger.Track("Generating command message."))
        {
            return _messageConverter.Serialize(context.Topic, null, JToken.FromObject(context), context.Connector);
        }
    }
}
