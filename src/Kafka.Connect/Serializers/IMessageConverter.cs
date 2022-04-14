using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Config.Models;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Serializers
{
    public interface IMessageConverter
    {
        Task<(JToken, JToken)> Deserialize(ConsumeResult<byte[], byte[]> consumed, string connector);
        Task<(JToken, JToken)> Deserialize(ConverterConfig config,  ConsumeResult<byte[], byte[]> consumed);

        Task<Message<byte[], byte[]>> Serialize(ConverterConfig converterConfig, TopicConfig topicConfig, JToken key,
            JToken value);
    }
}