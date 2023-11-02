using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Serializers;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Serializers;

public class JsonDeserializer : Deserializer
{
    private readonly ILogger<JsonDeserializer> _logger;

    public JsonDeserializer(ILogger<JsonDeserializer> logger)
    {
        _logger = logger;
    }

    public override async Task<JToken> Deserialize(ReadOnlyMemory<byte> data, string topic, IDictionary<string, byte[]> headers, bool isValue = true)
    {
        using (_logger.Track("Deserializing the record using json deserializer."))
        {
            JToken token;
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
                token = Newtonsoft.Json.JsonConvert.DeserializeObject<JToken>(await sr.ReadToEndAsync());
            }
            catch (AggregateException ae)
            {
                throw ae.InnerException ?? ae;
            }

            return Wrap(token, isValue);
        }
    }
}
