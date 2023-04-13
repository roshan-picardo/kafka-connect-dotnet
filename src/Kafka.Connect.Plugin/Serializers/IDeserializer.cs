using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Plugin.Serializers
{
    public interface IDeserializer
    {
        Task<JToken> Deserialize(ReadOnlyMemory<byte> data, string topic, IDictionary<string, byte[]> headers, bool isValue = true);

        bool IsOfType(string type);
    }
}