using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Config.Models;
using Kafka.Connect.Configurations;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Providers;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using ConverterConfig = Kafka.Connect.Config.Models.ConverterConfig;

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

        public async Task<(JToken, JToken)> Deserialize(ConsumeResult<byte[], byte[]> consumed, string connector)
        {
            var (keyConfig, valueConfig) = _configurationProvider.GetMessageConverters(connector, consumed.Topic);
            var keyToken =
                await _logger.Timed($"Deserializing record key using {keyConfig}")
                    .Execute(() => _processorServiceProvider.GetDeserializer(keyConfig)
                        .Deserialize(consumed.Message.Key, new SerializationContext(MessageComponentType.Key, consumed.Topic, consumed.Message?.Headers), consumed.Message.Key == null || consumed.Message.Key.Length == 0));
            var valueToken = 
                await _logger.Timed($"Deserializing record value using {valueConfig}")
                    .Execute(() => _processorServiceProvider.GetDeserializer(valueConfig)
                        .Deserialize(consumed.Message.Value, new SerializationContext(MessageComponentType.Value, consumed.Topic, consumed.Message?.Headers), consumed.Message.Value == null || consumed.Message.Value.Length == 0));
            return (keyToken, valueToken);
        }
        
        public async Task<(JToken, JToken)> Deserialize(ConverterConfig converterConfig,
            ConsumeResult<byte[], byte[]> consumed)
        {
            var keyConfig = converterConfig.Overrides?.SingleOrDefault(d => d.Topic == consumed.Topic)?.Key ??
                            converterConfig.Key;
            var valueConfig = converterConfig.Overrides?.SingleOrDefault(d => d.Topic == consumed.Topic)?.Value ??
                            converterConfig.Value;
            var keyToken =
                await _logger.Timed($"Deserializing record key using {keyConfig}")
                    .Execute(async () => await _processorServiceProvider.GetDeserializer(keyConfig)
                        .Deserialize(consumed.Message.Key, new SerializationContext(MessageComponentType.Key, consumed.Topic, consumed.Message?.Headers), consumed.Message.Key == null || consumed.Message.Key.Length == 0));
            var valueToken = 
                await _logger.Timed($"Deserializing record value using {valueConfig}")
                    .Execute(async () => await _processorServiceProvider.GetDeserializer(valueConfig)
                        .Deserialize(consumed.Message.Value, new SerializationContext(MessageComponentType.Value, consumed.Topic, consumed.Message?.Headers), consumed.Message.Value == null || consumed.Message.Value.Length == 0));
            return (keyToken, valueToken);
        }

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
        }
    }
}