using System.Collections.Generic;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Plugin.Serializers
{
    public interface ISerializer
    {
        Task<byte[]> Serialize<T>(JToken data, string topic, IDictionary<string, byte[]> headers, T schema, bool isValue = true);
        bool IsOfType(string type);
    }
}