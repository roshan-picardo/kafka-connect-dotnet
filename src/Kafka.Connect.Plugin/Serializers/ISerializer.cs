using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Plugin.Serializers
{
    public interface ISerializer
    {
        Task<byte[]> Serialize<T>(JToken data, SerializationContext context, T schema);
        bool IsOfType(string type);
    }
}