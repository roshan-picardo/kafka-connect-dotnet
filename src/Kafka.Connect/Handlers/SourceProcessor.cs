using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Serializers;
using Serilog.Context;
using Serilog.Core.Enrichers;
using IConfigurationProvider = Kafka.Connect.Providers.IConfigurationProvider;

namespace Kafka.Connect.Handlers;

public interface ISourceProcessor
{
    Task<IList<CommandContext>> Process(ConnectRecordBatch batch, string connector);
}

public class SourceProcessor : ISourceProcessor
{
    private readonly ILogger<SourceProcessor> _logger;
    private readonly IMessageConverter _messageConverter;
    private readonly IConfigurationProvider _configurationProvider;

    public SourceProcessor(
        ILogger<SourceProcessor> logger, 
        IMessageConverter messageConverter,
        IConfigurationProvider configurationProvider)
    {
        _logger = logger;
        _messageConverter = messageConverter;
        _configurationProvider = configurationProvider;
    }
    public async Task<IList<CommandContext>> Process(ConnectRecordBatch batch, string connector)
    {
        using (_logger.Track("Processing the batch."))
        {
            var sourceConfig = _configurationProvider.GetSourceConfig(connector);
            var trackBatch = new List<CommandContext>();
            batch ??= new ConnectRecordBatch(connector);
            foreach (var topicBatch in batch.GetByTopicPartition<Models.ConnectRecord>())
            {
                using (LogContext.Push(new PropertyEnricher(Constants.Topic, topicBatch.Topic),
                           new PropertyEnricher(Constants.Partition, topicBatch.Partition)))
                {
                    await topicBatch.Batch.OrderByDescending(r => r.Offset).ForEachAsync(async record =>
                        {
                            using (LogContext.PushProperty(Constants.Offset, record.Offset))
                            {
                                var (keyToken, valueToken) =
                                    await _messageConverter.Deserialize(record.Topic, record.GetConsumedMessage(),
                                        connector);
                                record.Parsed(keyToken, valueToken);
                                var context = record.GetValue<CommandContext>();
                                if (sourceConfig.Commands.ContainsKey(context.Topic) && 
                                    context.Connector == connector &&
                                    !trackBatch.Any<CommandContext>(c => c.Connector == connector && c.Topic == context.Topic))
                                {
                                    trackBatch.Add(context);
                                }

                                _logger.Document(record.Message);
                            }
                        }, (record, exception) => exception.SetLogContext(record),
                        _configurationProvider.GetBatchConfig(connector).Parallelism);
                }
            }

            foreach (var ((topic, command), index) in sourceConfig.Commands.Select((commands, i) => (commands, i))) 
            {
                if (!trackBatch.Exists(c => c.Topic == topic) && batch.GetEofPartitions().Any(p => p.Partition == index))
                {
                    trackBatch.Add(new CommandContext
                    {
                        Connector = connector,
                        Partition = index, 
                        Offset = -1,
                        Command = command,
                        Topic = topic
                    });
                }
            }

            return trackBatch;
        }
    }
}