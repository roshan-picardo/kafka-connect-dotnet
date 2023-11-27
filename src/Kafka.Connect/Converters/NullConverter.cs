using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Converters;
using Kafka.Connect.Plugin.Logging;

namespace Kafka.Connect.Converters;

public class NullConverter : IMessageConverter
{
    private readonly ILogger<NullConverter> _logger;

    public NullConverter(ILogger<NullConverter> logger)
    {
        _logger = logger;
    }
    
    public Task<byte[]> Serialize(string topic, JsonNode data, string subject = null, IDictionary<string, byte[]> headers = null, bool isValue = true)
    {
        using (_logger.Track($"Serializing the record {(isValue ? "value": "key")}."))
        {
            return Task.FromResult((byte[])null);
        }
    }

    public Task<JsonNode> Deserialize(string topic, ReadOnlyMemory<byte> data, IDictionary<string, byte[]> headers, bool isValue = true)
    {
        using (_logger.Track($"Deserializing the record {(isValue ? "value": "key")}."))
        {
            return Task.FromResult((JsonNode)null);
        }
    }
}