using System;
using System.Threading.Tasks;
using Avro.Generic;
using Confluent.Kafka;
using Kafka.Connect.Converters;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Serializers;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Serializers
{
    public class AvroDeserializer : Deserializer
    {
        private readonly IAsyncDeserializer<GenericRecord> _deserializer;
        private readonly IGenericRecordParser _genericRecordParser;
        private readonly ILogger<AvroDeserializer> _logger;

        public AvroDeserializer(IAsyncDeserializer<GenericRecord> deserializer, IGenericRecordParser genericRecordParser, ILogger<AvroDeserializer> logger)
        {
            _deserializer = deserializer;
            _genericRecordParser = genericRecordParser;
            _logger = logger;
        }

        [OperationLog("Deserializing the record using avro deserializer.")]
        public override async Task<JToken> Deserialize(ReadOnlyMemory<byte> data, SerializationContext context,
            bool isNull = false)
        {
            var record = await _deserializer.DeserializeAsync(data, isNull, context);
            return Wrap(_genericRecordParser.Parse(record), context);
        }
    }
}