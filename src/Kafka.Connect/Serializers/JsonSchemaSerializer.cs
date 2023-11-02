using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Serializers;
using Kafka.Connect.Utilities;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Serializers;

public class JsonSchemaSerializer : Serializer
{
    private readonly ILogger<JsonSchemaSerializer> _logger;
    private readonly IAsyncSerializer<JToken> _serializer;

    public JsonSchemaSerializer(ILogger<JsonSchemaSerializer> logger, IAsyncSerializer<JToken> serializer)
    {
        _logger = logger;
        _serializer = serializer;
    }

    public override async Task<byte[]> Serialize(string topic, JToken data, string subject = null, IDictionary<string, byte[]> headers = null, bool isValue = true)
    {
        using (_logger.Track("Serializing the record using avro serializer."))
        {
            var context = new SerializationContext(isValue ? MessageComponentType.Value : MessageComponentType.Key,
                topic, headers?.ToMessageHeaders());
            var serialized = await _serializer.SerializeAsync(data, context);
            return serialized;
        }
    }
}
