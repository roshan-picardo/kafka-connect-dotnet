using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Plugin.Serializers
{
    public abstract class Deserializer : IDeserializer
    {
        public abstract Task<JToken> Deserialize(ReadOnlyMemory<byte> data, string topic, IDictionary<string, byte[]> headers, bool isValue = true);
        public bool IsOfType(string type)
        {
            return GetType().FullName == type;
        }

        protected static JToken Wrap(JToken token, bool isValue)
        {
            var component = isValue ? "value" : "key";
            return new JObject {{component, token}};
        }
    }
}