using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Converters;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Utilities;

namespace Kafka.Connect.Converters;

public class AvroConverter : IMessageConverter
{
    private readonly ILogger<AvroConverter> _logger;
    private readonly IAsyncSerializer<GenericRecord> _serializer;
    private readonly IGenericRecordHandler _genericRecordHandler;
    private readonly IAsyncDeserializer<GenericRecord> _deserializer;
    private readonly ISchemaRegistryClient _schemaRegistryClient;

    public AvroConverter(
        ILogger<AvroConverter> logger,
        IAsyncSerializer<GenericRecord> serializer,
        IGenericRecordHandler genericRecordHandler,
        IAsyncDeserializer<GenericRecord> deserializer,
        ISchemaRegistryClient schemaRegistryClient)
    {
        _logger = logger;
        _serializer = serializer;
        _genericRecordHandler = genericRecordHandler;
        _deserializer = deserializer;
        _schemaRegistryClient = schemaRegistryClient;
    }

    public async Task<byte[]> Serialize(string topic, JsonNode data, string subject = null, IDictionary<string, byte[]> headers = null, bool isValue = true)
    {
        using (_logger.Track($"Serializing the record {(isValue ? "value": "key")}."))
        {
            var context = new SerializationContext(isValue ? MessageComponentType.Value : MessageComponentType.Key,
                topic, headers?.ToMessageHeaders());
            var serialized = await _serializer.SerializeAsync(
                _genericRecordHandler.Build(await GetRecordSchema(subject), data), context);
            return serialized;
        }
    }

    public async Task<JsonNode> Deserialize(string topic, ReadOnlyMemory<byte> data, IDictionary<string, byte[]> headers, bool isValue = true)
    {
        using (_logger.Track($"Deserializing the record {(isValue ? "value": "key")}."))
        {
            var isNull = data.IsEmpty || data.Length == 0;
            var context = new SerializationContext(isValue ? MessageComponentType.Value : MessageComponentType.Key,
                topic, headers.ToMessageHeaders());
            var record = await _deserializer.DeserializeAsync(data, isNull, context);
            return _genericRecordHandler.Parse(record);
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