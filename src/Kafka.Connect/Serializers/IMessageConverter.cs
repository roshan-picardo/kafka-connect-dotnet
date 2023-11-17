using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Models;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Serializers;

public interface IMessageConverter
{
    Task<ConnectMessage<JToken>> Deserialize(string topic, ConnectMessage<byte[]> message, string connector);
    Task<Message<byte[], byte[]>> Serialize(string topic, JToken key, JToken value, string connector);
    Task<ConnectMessage<byte[]>> Serialize(string connector, string topic, ConnectMessage<JsonNode> message);
    Task<ConnectMessage<JsonNode>> Deserialize(string connector, string topic, ConnectMessage<byte[]> message);
}
