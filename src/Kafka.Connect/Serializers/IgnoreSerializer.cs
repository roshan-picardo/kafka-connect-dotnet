using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Serializers;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Serializers;

public class IgnoreSerializer : Serializer
{
    private readonly ILogger<IgnoreSerializer> _logger;

    public IgnoreSerializer(ILogger<IgnoreSerializer> logger)
    {
        _logger = logger;
    }
        
    public override Task<byte[]> Serialize(string topic, JToken data, string subject = null, IDictionary<string, byte[]> headers = null, bool isValue = true)
    {
        using (_logger.Track("Ignoring serialization of the data."))
        {
            return Task.FromResult((byte[])null);
        }
    }
}
