using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Serializers;

public interface IMessageConverter
{
    Task<(JToken Key, JToken Value)> Deserialize(string topic, Message<byte[], byte[]> message, string connector);
    Task<Message<byte[], byte[]>> Serialize(string topic, JToken key, JToken value, string connector);
}
