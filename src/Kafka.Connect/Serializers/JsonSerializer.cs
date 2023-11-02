using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Serializers;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Serializers;

public class JsonSerializer : Serializer
{
    private readonly ILogger<JsonSerializer> _logger;

    public JsonSerializer(ILogger<JsonSerializer> logger)
    {
        _logger = logger;
    }

    public override Task<byte[]> Serialize(string topic, JToken data, string subject = null, IDictionary<string, byte[]> headers = null, bool isValue = true)
    {
        using (_logger.Track("Serializing the record using json serializer."))
        {
            return Task.FromResult(data.ToObject<byte[]>());
        }
    }
}
