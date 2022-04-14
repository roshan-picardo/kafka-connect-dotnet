using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Kafka.Connect.Converters;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Serializers;
using Kafka.Connect.Schemas;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Serializers
{
    public class AvroSerializer : Serializer
    {
        private readonly IAsyncSerializer<GenericRecord> _serializer;
        private readonly IGenericRecordBuilder _genericRecordBuilder;
        private readonly ISchemaRegistryClient _schemaRegistryClient;

        public AvroSerializer(IAsyncSerializer<GenericRecord> serializer, IGenericRecordBuilder genericRecordBuilder,
            IEnumerable<ISchemaRegistryClient> schemaRegistryClients)
        {
            _serializer = serializer;
            _genericRecordBuilder = genericRecordBuilder;
            _schemaRegistryClient =
                schemaRegistryClients.FirstOrDefault(client => client is ProducerCachedSchemaRegistryClient);
        }

        public override async Task<byte[]> Serialize<T>(JToken data, SerializationContext context, T schema)
        {
            var serialized = await _serializer.SerializeAsync(
                _genericRecordBuilder.Build(await GetRecordSchema(schema), data), context);
            return serialized;
        }

        private async Task<RecordSchema> GetRecordSchema<T>(T searchKey)
        {
            try
            {
                var schemaString = searchKey switch
                {
                    string subject => (await _schemaRegistryClient.GetLatestSchemaAsync(subject)).SchemaString,
                    int id => (await _schemaRegistryClient.GetSchemaAsync(id)).SchemaString,
                    _ => throw new SchemaParseException($"Schema not found, for search key: {searchKey}")
                };
                return Avro.Schema.Parse(schemaString) as RecordSchema;
            }
            catch (Exception ex)
            {
                throw new ConnectDataException(ErrorCode.Local_Fail, ex);
            }
        }

    }
}