using System.Collections.Generic;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Plugin.Serializers;

public abstract class Serializer : ISerializer
{
    public abstract Task<byte[]> Serialize(
        string topic,
        JToken data,
        string subject = null,
        IDictionary<string, byte[]> headers = null,
        bool isValue = true);

    public bool IsOfType(string type)
    {
        return GetType().FullName == type;
    }
}
