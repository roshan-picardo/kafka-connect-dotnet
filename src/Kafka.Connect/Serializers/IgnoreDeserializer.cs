using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Serializers;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Serializers
{
    public class IgnoreDeserializer : Deserializer
    {
        private readonly ILogger<IgnoreDeserializer> _logger;

        public IgnoreDeserializer(ILogger<IgnoreDeserializer> logger)
        {
            _logger = logger;
        }
        public override async Task<JToken> Deserialize(ReadOnlyMemory<byte> data, SerializationContext context, bool isNull = false)
        {
            using (_logger.Track("Ignoring the deserialization of the record."))
            {
                return await Task.FromResult(Wrap(null, context));
            }
        }
    }
}