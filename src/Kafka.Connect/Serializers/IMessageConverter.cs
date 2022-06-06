using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Serializers
{
    public interface IMessageConverter
    {
        Task<(JToken Key, JToken Value)> Deserialize(ConsumeResult<byte[], byte[]> consumed, string connector);
    }
}