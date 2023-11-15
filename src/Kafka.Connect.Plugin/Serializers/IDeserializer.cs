using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Kafka.Connect.Plugin.Serializers
{
    public interface IDeserializer
    {
        Task<JsonNode> Deserialize(ReadOnlyMemory<byte> data, string topic, IDictionary<string, byte[]> headers, bool isValue = true);

        bool IsOfType(string type);
    }
}