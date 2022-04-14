using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Plugin.Serializers
{
    public interface IDeserializer
    {
        Task<JToken> Deserialize(ReadOnlyMemory<byte> data, SerializationContext context, bool isNull = false);
        bool IsOfType(string type);
    }
}