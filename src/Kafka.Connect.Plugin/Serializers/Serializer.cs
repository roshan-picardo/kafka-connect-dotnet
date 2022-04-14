using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Plugin.Serializers
{
    public abstract class Serializer : ISerializer
    {
        public abstract Task<byte[]> Serialize<T>(JToken data, SerializationContext context, T schema);

        public bool IsOfType(string type)
        {
            return GetType().FullName == type;
        }
    }
}