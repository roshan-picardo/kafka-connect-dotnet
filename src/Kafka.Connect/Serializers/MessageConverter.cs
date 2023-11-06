using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Serializers;

public class MessageConverter : IMessageConverter
{
    private readonly ILogger<MessageConverter> _logger;
    private readonly IProcessorServiceProvider _processorServiceProvider;
    private readonly IConfigurationProvider _configurationProvider;

    public MessageConverter(ILogger<MessageConverter> logger, IProcessorServiceProvider processorServiceProvider, IConfigurationProvider configurationProvider)
    {
        _logger = logger;
        _processorServiceProvider = processorServiceProvider;
        _configurationProvider = configurationProvider;
    }

    public async Task<ConnectMessage<JToken>> Deserialize(string topic, ConnectMessage<byte[]> message, string connector)
    {
        using (_logger.Track("Deserializing the message."))
        {
            var converterConfig = _configurationProvider.GetDeserializers(connector, topic);
            var deserialized = new ConnectMessage<JToken>
            {
                Key = await _processorServiceProvider.GetDeserializer(converterConfig.Key).Deserialize(message.Key, topic, message.Headers, false),
                Value = await _processorServiceProvider.GetDeserializer(converterConfig.Value)
                    .Deserialize(message.Value, topic, message.Headers)
            };
            return deserialized;
        }
    }

    public async Task<Message<byte[], byte[]>> Serialize(string topic, JToken key, JToken value, string connector)
    {
        using (_logger.Track("Serializing the message."))
        {
            var converterConfig = _configurationProvider.GetSerializers(connector, topic);
            var schemaSubject = Enum.Parse<SubjectNameStrategy>(converterConfig.Subject).ToDelegate()(
                new SerializationContext(MessageComponentType.Key, topic), converterConfig.Record);

            var message = new Message<byte[], byte[]>
            {
                Key = await _processorServiceProvider.GetSerializer(converterConfig.Key)
                    .Serialize(topic, key, schemaSubject),
                Value = await _processorServiceProvider.GetSerializer(converterConfig.Value)
                    .Serialize(topic, value, schemaSubject)
            };

            return message;
        }
    }
}
