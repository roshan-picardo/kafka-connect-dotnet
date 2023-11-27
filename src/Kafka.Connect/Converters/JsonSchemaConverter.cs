using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Converters;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Utilities;

namespace Kafka.Connect.Converters;

public class JsonSchemaConverter : IMessageConverter
{
    private readonly ILogger<JsonSchemaConverter> _logger;
    private readonly IAsyncSerializer<JsonNode> _serializer;
    private readonly IAsyncDeserializer<JsonNode> _deserializer;

    public JsonSchemaConverter(
        ILogger<JsonSchemaConverter> logger,
        IAsyncSerializer<JsonNode> serializer,
        IAsyncDeserializer<JsonNode> deserializer)
    {
        _logger = logger;
        _serializer = serializer;
        _deserializer = deserializer;
    }

    public async Task<byte[]> Serialize(string topic, JsonNode data, string subject = null, IDictionary<string, byte[]> headers = null, bool isValue = true)
    {
        using (_logger.Track($"Serializing the record {(isValue ? "value" : "key")}."))
        {
            var context = new SerializationContext(isValue ? MessageComponentType.Value : MessageComponentType.Key,
                topic, headers?.ToMessageHeaders());
            var serialized = await _serializer.SerializeAsync(data, context);
            return serialized;
        }
    }

    public async Task<JsonNode> Deserialize(string topic, ReadOnlyMemory<byte> data, IDictionary<string, byte[]> headers, bool isValue = true)
    {
        using (_logger.Track($"Deserializing the record {(isValue ? "value" : "key")}."))
        {
            var isNull = data.IsEmpty || data.Length == 0;
            var context = new SerializationContext(isValue ? MessageComponentType.Value : MessageComponentType.Key,
                topic, headers.ToMessageHeaders());
            return await _deserializer.DeserializeAsync(data, isNull, context);
        }
    }
}