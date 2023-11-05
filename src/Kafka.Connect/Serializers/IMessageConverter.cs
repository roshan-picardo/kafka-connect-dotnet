using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Models;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Serializers;

public interface IMessageConverter
{
    Task<ConnectMessage<JToken, JToken>> Deserialize(string topic, Message<byte[], byte[]> message, string connector);
    Task<Message<byte[], byte[]>> Serialize(string topic, JToken key, JToken value, string connector);
}
