using Avro.Generic;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Converters
{
    public interface IGenericRecordParser
    {
        JToken Parse(GenericRecord genericRecord);
    }
}