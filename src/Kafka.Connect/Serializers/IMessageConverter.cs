using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Serializers;

public interface IMessageConverter
{
    Task<ConnectMessage<byte[]>> Serialize(string connector, string topic, ConnectMessage<JsonNode> message);
    Task<ConnectMessage<JsonNode>> Deserialize(string connector, string topic, ConnectMessage<byte[]> message);
}
