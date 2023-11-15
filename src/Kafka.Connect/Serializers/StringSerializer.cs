using System.Collections.Generic;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Serializers;

namespace Kafka.Connect.Serializers;

public class StringSerializer : Serializer
{
    private readonly ILogger<StringSerializer> _logger;

    public StringSerializer(ILogger<StringSerializer> logger)
    {
        _logger = logger;
    }
        
    public override Task<byte[]> Serialize(string topic, JsonNode data, string subject = null, IDictionary<string, byte[]> headers = null, bool isValue = true)
    {
        using (_logger.Track("Serializing the record using string serializer."))
        {
            return Task.FromResult(Encoding.UTF8.GetBytes(data.ToJsonString()));
        }
    }
}
