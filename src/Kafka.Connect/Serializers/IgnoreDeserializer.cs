using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Serializers;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Serializers;

public class IgnoreDeserializer : Deserializer
{
    private readonly ILogger<IgnoreDeserializer> _logger;

    public IgnoreDeserializer(ILogger<IgnoreDeserializer> logger)
    {
        _logger = logger;
    }

    public override Task<JToken> Deserialize(ReadOnlyMemory<byte> data, string topic, IDictionary<string, byte[]> headers, bool isValue = true)
    {
        using (_logger.Track("Ignoring the deserialization of the record."))
        {
            return Task.FromResult(Wrap(null, isValue));
        }
    }
}
