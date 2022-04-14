using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Serializers;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Serializers
{
    public class IgnoreDeserializer : Deserializer
    {
        public override async Task<JToken> Deserialize(ReadOnlyMemory<byte> data, SerializationContext context, bool isNull = false)
        {
            return await Task.FromResult(Wrap(null, context));
        }
    }
}