using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Serializers;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Serializers
{
    public class JsonSchemaDeserializer : Deserializer
    {
        private readonly IAsyncDeserializer<JObject> _deserializer;
        private readonly ILogger<JsonSchemaDeserializer> _logger;

        public JsonSchemaDeserializer(IAsyncDeserializer<JObject> deserializer, ILogger<JsonSchemaDeserializer> logger)
        {
            _deserializer = deserializer;
            _logger = logger;
        }

        public override async Task<JToken> Deserialize(ReadOnlyMemory<byte> data, SerializationContext context,
            bool isNull = false)
        {
            return Wrap(
                await _logger.Timed("Deserializing the record.")
                    .Execute(async () => await _deserializer.DeserializeAsync(data, isNull, context)), context);
        }
    }
}