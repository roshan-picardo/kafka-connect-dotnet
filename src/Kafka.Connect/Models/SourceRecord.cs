using Kafka.Connect.Plugin.Models;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Models;

public class SourceRecord : Plugin.Models.ConnectRecord
{
    public SourceRecord(string topic, JToken key, JToken value) : base(topic, -1, -1)
    {
        DeserializedToken = new ConnectMessage<JToken>
        {
            Key = key,
            Value = value
        };
        StartTiming();
    }
}