using System.Text.Json.Nodes;
using Avro;
using Avro.Generic;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Converters
{
    public interface IGenericRecordBuilder
    {
        GenericRecord Build(Schema schema, JsonNode data);
    }
}