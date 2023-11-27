using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Kafka.Connect.Plugin.Serializers;

public interface ISerializer
{
    Task<byte[]> Serialize(
        string topic,
        JsonNode data,
        string subject = null,
        IDictionary<string, byte[]> headers = null,
        bool isValue = true);
}
