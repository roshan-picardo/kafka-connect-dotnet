using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Providers;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Serializers
{
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

        public async Task<(JToken, JToken)> Deserialize(string topic, Message<byte[], byte[]> message, string connector)
        {
            using (_logger.Track("Deserializing the message."))
            {
                var (keyConfig, valueConfig) = _configurationProvider.GetMessageConverters(connector, topic);
                var keyToken =
                    await _processorServiceProvider.GetDeserializer(keyConfig).Deserialize(message.Key,
                        topic, message?.Headers?.ToDictionary(h => h.Key, h => h.GetValueBytes()),
                        false);
                var valueToken =
                    await _processorServiceProvider.GetDeserializer(valueConfig)
                        .Deserialize(message?.Value, topic,
                            message?.Headers?.ToDictionary(h => h.Key, h => h.GetValueBytes()));
                return (keyToken, valueToken);
            }
        }

        /* TODO: may need to port it to somewhere else
        public async Task<Message<byte[], byte[]>> Serialize(ConverterConfig converterConfig, TopicConfig topicConfig, JToken key, JToken value)
        {
            var keyConfig = converterConfig.Overrides?.SingleOrDefault(d => d.Topic == topicConfig.Name)?.Key ??
                            converterConfig.Key;
            var valueConfig = converterConfig.Overrides?.SingleOrDefault(d => d.Topic == topicConfig.Name)?.Value ??
                              converterConfig.Value;
            _logger.LogTrace("{@debug}", new {message = $"Serializers: [(Key: {keyConfig}), (Value: {valueConfig})]"});
            var message = new Message<byte[], byte[]>
            {
                Key = await _logger.Timed($"Serializing record key using {keyConfig}")
                    .Execute(async () => await _processorServiceProvider.GetSerializer(keyConfig)
                        .Serialize(key, SerializationContext.Empty, topicConfig.GetKeySchemaSubjectOrId())),
                Value = await _logger.Timed($"Serializing record value using {valueConfig}")
                    .Execute(async () => await _processorServiceProvider.GetSerializer(keyConfig)
                        .Serialize(value, SerializationContext.Empty, topicConfig.GetValueSchemaSubjectOrId()))
            };
            return message;
        } */
    }
}