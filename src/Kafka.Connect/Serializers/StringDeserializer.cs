using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Serializers;

namespace Kafka.Connect.Serializers;

public class StringDeserializer : Deserializer
{
    private readonly ILogger<StringDeserializer> _logger;

    public StringDeserializer(ILogger<StringDeserializer> logger)
    {
        _logger = logger;
    }

    public override async Task<JsonNode> Deserialize(ReadOnlyMemory<byte> data, string topic, IDictionary<string, byte[]> headers, bool isValue = true)
    {
        using (_logger.Track("Deserializing the record using string deserializer."))
        {
            string strData;
            var isNull = data.IsEmpty || data.Length == 0;
            if (isNull || data.IsEmpty) return Wrap(null, isValue);
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
                strData = await sr.ReadToEndAsync();
            }
            catch (AggregateException ae)
            {
                throw ae.InnerException ?? ae;
            }

            return Wrap(strData, isValue);
        }
    }
}
