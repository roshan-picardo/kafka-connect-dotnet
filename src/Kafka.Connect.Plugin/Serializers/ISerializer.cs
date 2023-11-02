using System.Collections.Generic;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Plugin.Serializers;

public interface ISerializer
{
    Task<byte[]> Serialize(
        string topic,
        JToken data,
        string subject = null,
        IDictionary<string, byte[]> headers = null,
        bool isValue = true);
        
    bool IsOfType(string type);
}
