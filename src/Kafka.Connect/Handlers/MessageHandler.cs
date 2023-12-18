using System;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;

namespace Kafka.Connect.Handlers;

public class MessageHandler : IMessageHandler
{
    private readonly ILogger<MessageHandler> _logger;
    private readonly IProcessorServiceProvider _processorServiceProvider;
    private readonly IConfigurationProvider _configurationProvider;

    public MessageHandler(
        ILogger<MessageHandler> logger,
        IProcessorServiceProvider processorServiceProvider,
        IConfigurationProvider configurationProvider)
    {
        _logger = logger;
        _processorServiceProvider = processorServiceProvider;
        _configurationProvider = configurationProvider;
    }

    public async Task<(bool Skip, ConnectMessage<JsonNode> Message)> Process(string connector, string topic,  ConnectMessage<JsonNode> deserialized)
    {
        using (_logger.Track("Processing the message."))
        {
            var configs = _configurationProvider.GetMessageProcessors(connector, topic);
            if (!(configs?.Any() ?? false))
            {
                return (false, deserialized);
            }

            var processors = _processorServiceProvider.GetProcessors()?.ToList();
            if (!(processors?.Any() ?? false))
            {
                return(false, deserialized);
            }

            var skip = false;
            var flattened = deserialized.Convert();

            foreach (var config in configs.OrderBy(p => p.Order))
            {
                var processor = processors.SingleOrDefault(p => p.Is(config.Name));
                if (processor == null)
                {
                    _logger.Trace("Processor is not registered.", new { Processor = config.Name });
                    continue;
                }

                (skip, flattened) = await processor.Apply(connector, flattened);
                if (!skip) continue;
                _logger.Trace("Message will be skipped from further processing.");
                break;
            }

            return(skip, flattened.Convert());
        }
    }

    public async Task<ConnectMessage<byte[]>> Serialize(string connector, string topic, ConnectMessage<JsonNode> message)
    {
        using (_logger.Track("Serializing the message."))
        {
            var converterConfig = _configurationProvider.GetMessageConverters(connector, topic);
            var schemaSubject = Enum.Parse<SubjectNameStrategy>(converterConfig.Subject).ToDelegate()(
                new SerializationContext(MessageComponentType.Key, topic), converterConfig.Record);

            return new ConnectMessage<byte[]>
            {
                Key = await _processorServiceProvider.GetMessageConverter(converterConfig.Key)
                    .Serialize(topic, message.Key, schemaSubject),
                Value = await _processorServiceProvider.GetMessageConverter(converterConfig.Value)
                    .Serialize(topic, message.Value, schemaSubject)
            };
        }
    }

    public async Task<ConnectMessage<JsonNode>> Deserialize(string connector, string topic, ConnectMessage<byte[]> message)
    {
        using (_logger.Track("Deserializing the message."))
        {
            var converterConfig = _configurationProvider.GetMessageConverters(connector, topic);
            
            return new ConnectMessage<JsonNode>
            {
                Key = await _processorServiceProvider.GetMessageConverter(converterConfig.Key)
                    .Deserialize(topic, message.Key, message.Headers, false) ?? JsonNode.Parse("{}"),
                Value = await _processorServiceProvider.GetMessageConverter(converterConfig.Value)
                    .Deserialize(topic, message.Value, message.Headers)?? JsonNode.Parse("{}")
            };
        }
    }
}