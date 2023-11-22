using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Avro.Generic;
using Confluent.Kafka;
using Kafka.Connect.Converters;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Serializers;
using Kafka.Connect.Utilities;

namespace Kafka.Connect.Serializers;

public class AvroDeserializer : IDeserializer
{
    private readonly IAsyncDeserializer<GenericRecord> _deserializer;
    private readonly IGenericRecordParser _genericRecordParser;
    private readonly ILogger<AvroDeserializer> _logger;

    public AvroDeserializer(ILogger<AvroDeserializer> logger, IAsyncDeserializer<GenericRecord> deserializer, IGenericRecordParser genericRecordParser)
    {
        _deserializer = deserializer;
        _genericRecordParser = genericRecordParser;
        _logger = logger;
    }

    public async Task<JsonNode> Deserialize(ReadOnlyMemory<byte> data, string topic, IDictionary<string, byte[]> headers, bool isValue = true)
    {
        using (_logger.Track("Deserializing the record using avro deserializer."))
        {
            var isNull = data.IsEmpty || data.Length == 0;
            var context = new SerializationContext(isValue ? MessageComponentType.Value : MessageComponentType.Key,
                topic, headers.ToMessageHeaders());
            var record = await _deserializer.DeserializeAsync(data, isNull, context);
            return _genericRecordParser.Parse(record);
        }
    }
}