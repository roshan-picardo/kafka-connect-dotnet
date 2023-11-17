using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Serializers;
using Kafka.Connect.Utilities;

namespace Kafka.Connect.Serializers
{
    public class JsonSchemaDeserializer : Deserializer
    {
        private readonly IAsyncDeserializer<JsonNode> _deserializer;
        private readonly ILogger<JsonSchemaDeserializer> _logger;

        public JsonSchemaDeserializer(ILogger<JsonSchemaDeserializer> logger, IAsyncDeserializer<JsonNode> deserializer)
        {
            _deserializer = deserializer;
            _logger = logger;
        }

        public override async Task<JsonNode> Deserialize(ReadOnlyMemory<byte> data, string topic, IDictionary<string, byte[]> headers, bool isValue = true)
        {
            using (_logger.Track("Deserializing the record using json schema deserializer."))
            {
                var isNull = data.IsEmpty || data.Length == 0;
                var context = new SerializationContext(isValue ? MessageComponentType.Value : MessageComponentType.Key,
                    topic, headers.ToMessageHeaders());
                return await _deserializer.DeserializeAsync(data, isNull, context);
            }
        }
    }
}