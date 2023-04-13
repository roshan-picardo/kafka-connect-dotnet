using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Avro.Generic;
using Confluent.Kafka;
using Kafka.Connect.Converters;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Serializers;
using Kafka.Connect.Utilities;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Serializers
{
    public class AvroDeserializer : Deserializer
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

        public override async Task<JToken> Deserialize(ReadOnlyMemory<byte> data, string topic, IDictionary<string, byte[]> headers, bool isValue = true)
        {
            using (_logger.Track("Deserializing the record using avro deserializer."))
            {
                var isNull = data.IsEmpty || data.Length == 0;
                var context = new SerializationContext(isValue ? MessageComponentType.Value : MessageComponentType.Key,
                    topic, headers.ToMessageHeaders());
                var record = await _deserializer.DeserializeAsync(data, isNull, context);
                return Wrap(_genericRecordParser.Parse(record), isValue);
            }
        }
    }
}