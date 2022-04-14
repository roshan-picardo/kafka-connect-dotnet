using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Plugin.Serializers
{
    public abstract class Deserializer : IDeserializer
    {
        public abstract Task<JToken> Deserialize(ReadOnlyMemory<byte> data, SerializationContext context,  bool isNull = false);
        public bool IsOfType(string type)
        {
            return GetType().FullName == type;
        }

        protected static JToken Wrap(JToken token, SerializationContext context)
        {
            var component = context.Component == 0 ? "value" : context.Component.ToString().ToLower();
            return new JObject {{component, token}};
        }
    }
}