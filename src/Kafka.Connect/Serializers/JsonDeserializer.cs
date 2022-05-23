using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Serializers;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Serializers
{
    public class JsonDeserializer : Deserializer
    {
        private const int HeaderSize = sizeof(int) + sizeof(byte);

        [OperationLog("Deserializing the record using json deserializer.")]
        public override async Task<JToken> Deserialize(ReadOnlyMemory<byte> data, SerializationContext context, bool isNull = false)
        {
            JToken token;

            if (isNull || data.IsEmpty) return Wrap(null, context);
            try
            {
                var array = data.ToArray();

                if (array.Length < 5)
                {
                    throw new InvalidDataException(
                        $"Expecting data framing of length 5 bytes or more but total data size is {array.Length} bytes");
                }

                await using var stream = new MemoryStream(array, HeaderSize, array.Length - HeaderSize);
                using var sr = new StreamReader(stream, Encoding.UTF8);
                token = Newtonsoft.Json.JsonConvert.DeserializeObject<JToken>(await sr.ReadToEndAsync());
            }
            catch (AggregateException ae)
            {
                throw ae.InnerException ?? ae;
            }

            return Wrap(token, context);
        }
    }
}