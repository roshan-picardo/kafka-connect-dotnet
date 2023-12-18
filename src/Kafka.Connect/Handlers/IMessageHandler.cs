using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Handlers;

public interface IMessageHandler
{
    Task<(bool Skip, ConnectMessage<JsonNode> Message)> Process(string connector, string topic, ConnectMessage<JsonNode> deserialized);
    Task<ConnectMessage<byte[]>> Serialize(string connector, string topic, ConnectMessage<JsonNode> message);
    Task<ConnectMessage<JsonNode>> Deserialize(string connector, string topic, ConnectMessage<byte[]> message);
}