using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Serializers;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Serializers
{
    public class JsonSchemaDeserializer : Deserializer
    {
        private readonly IAsyncDeserializer<JObject> _deserializer;
        private readonly ILogger<JsonSchemaDeserializer> _logger;

        public JsonSchemaDeserializer(ILogger<JsonSchemaDeserializer> logger, IAsyncDeserializer<JObject> deserializer)
        {
            _deserializer = deserializer;
            _logger = logger;
        }

        public override async Task<JToken> Deserialize(ReadOnlyMemory<byte> data, SerializationContext context,
            bool isNull = false)
        {
            using (_logger.Track("Deserializing the record using json schema deserializer."))
            {
                return Wrap(await _deserializer.DeserializeAsync(data, isNull, context), context);
            }
        }
    }
}