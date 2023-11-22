using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Kafka.Connect.Converters;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Serializers;
using Kafka.Connect.Utilities;

namespace Kafka.Connect.Serializers;

public class AvroSerializer : ISerializer
{
    private readonly ILogger<AvroSerializer> _logger;
    private readonly IAsyncSerializer<GenericRecord> _serializer;
    private readonly IGenericRecordBuilder _genericRecordBuilder;
    private readonly ISchemaRegistryClient _schemaRegistryClient;

    public AvroSerializer(
        ILogger<AvroSerializer> logger,
        IAsyncSerializer<GenericRecord> serializer,
        IGenericRecordBuilder genericRecordBuilder,
        ISchemaRegistryClient schemaRegistryClient)
    {
        _logger = logger;
        _serializer = serializer;
        _genericRecordBuilder = genericRecordBuilder;
        _schemaRegistryClient = schemaRegistryClient;
    }
        
    public async Task<byte[]> Serialize(string topic, JsonNode data, string subject = null, IDictionary<string, byte[]> headers = null, bool isValue = true)
    {
        using (_logger.Track("Serializing the record using avro serializer."))
        {
            var context = new SerializationContext(isValue ? MessageComponentType.Value : MessageComponentType.Key,
                topic, headers?.ToMessageHeaders());
            var serialized = await _serializer.SerializeAsync(
                _genericRecordBuilder.Build(await GetRecordSchema(subject), data), context);
            return serialized;
        }
    }

    private async Task<RecordSchema> GetRecordSchema(string subject)
    {
        try
        {
            var schemaString = (await _schemaRegistryClient.GetLatestSchemaAsync(subject)).Schema.SchemaString;
            return Avro.Schema.Parse(schemaString) as RecordSchema;
        }
        catch (Exception ex)
        {
            throw new ConnectDataException(ErrorCode.Local_Fail.GetReason(), ex);
        }
    }
}
