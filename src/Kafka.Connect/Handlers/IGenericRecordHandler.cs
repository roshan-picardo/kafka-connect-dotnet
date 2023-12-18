using System.Text.Json.Nodes;
using Avro;
using Avro.Generic;

namespace Kafka.Connect.Handlers;

public interface IGenericRecordHandler
{
    GenericRecord Build(Schema schema, JsonNode data);
    JsonNode Parse(GenericRecord genericRecord);
}