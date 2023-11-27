using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Converters;
using Kafka.Connect.Plugin.Logging;

namespace Kafka.Connect.Converters;

public class JsonConverter : IMessageConverter
{
    private readonly ILogger<JsonConverter> _logger;

    public JsonConverter(ILogger<JsonConverter> logger)
    {
        _logger = logger;
    }
    
    public Task<byte[]> Serialize(string topic, JsonNode data, string subject = null, IDictionary<string, byte[]> headers = null, bool isValue = true)
    {
        using (_logger.Track($"Serializing the record {(isValue ? "value" : "key")}."))
        {
            return Task.FromResult(data.GetValue<byte[]>());
        }
    }

    public async Task<JsonNode> Deserialize(string topic, ReadOnlyMemory<byte> data, IDictionary<string, byte[]> headers, bool isValue = true)
    {
        using (_logger.Track($"Deserializing the record {(isValue ? "value" : "key")}."))
        {
            JsonNode token;
            var isNull = data.IsEmpty || data.Length == 0;
            if (isNull || data.IsEmpty) return null;
            try
            {
                var array = data.ToArray();

                if (array.Length < 5)
                {
                    throw new InvalidDataException(
                        $"Expecting data framing of length 5 bytes or more but total data size is {array.Length} bytes");
                }

                await using var stream = new MemoryStream(array, 0, array.Length);
                using var sr = new StreamReader(stream, Encoding.UTF8);
                token = JsonNode.Parse(await sr.ReadToEndAsync());
            }
            catch (AggregateException ae)
            {
                throw ae.InnerException ?? ae;
            }

            return token;
        }
    }
}