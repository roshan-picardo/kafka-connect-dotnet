using System.Text.Json.Nodes;
using Avro;
using Avro.Generic;

namespace Kafka.Connect.Converters
{
    public interface IGenericRecordBuilder
    {
        GenericRecord Build(Schema schema, JsonNode data);
    }
}