using System.Collections.Generic;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Plugin.Serializers
{
    public abstract class Serializer : ISerializer
    {
        public abstract Task<byte[]> Serialize<T>(JToken data, string topic, IDictionary<string, byte[]> headers, T schema, bool isValue = true);

        public bool IsOfType(string type)
        {
            return GetType().FullName == type;
        }
    }
}