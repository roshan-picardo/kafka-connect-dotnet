using System.Text.Json.Nodes;
using Avro.Generic;

namespace Kafka.Connect.Converters.Generic;

public interface IGenericRecordParser
{
    JsonNode Parse(GenericRecord genericRecord);
}