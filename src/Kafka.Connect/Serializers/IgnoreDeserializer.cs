using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Serializers;

namespace Kafka.Connect.Serializers;

public class IgnoreDeserializer : Deserializer
{
    private readonly ILogger<IgnoreDeserializer> _logger;

    public IgnoreDeserializer(ILogger<IgnoreDeserializer> logger)
    {
        _logger = logger;
    }

    public override Task<JsonNode> Deserialize(ReadOnlyMemory<byte> data, string topic, IDictionary<string, byte[]> headers, bool isValue = true)
    {
        using (_logger.Track("Ignoring the deserialization of the record."))
        {
            return Task.FromResult(Wrap(null, isValue));
        }
    }
}
