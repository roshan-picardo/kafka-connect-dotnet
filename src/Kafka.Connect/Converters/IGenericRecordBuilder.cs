using Avro;
using Avro.Generic;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Converters
{
    public interface IGenericRecordBuilder
    {
        GenericRecord Build(Schema schema, JToken data);
    }
}