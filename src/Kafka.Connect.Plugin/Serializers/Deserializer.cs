using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Kafka.Connect.Plugin.Serializers;

public abstract class Deserializer : IDeserializer
{
    public abstract Task<JsonNode> Deserialize(ReadOnlyMemory<byte> data, string topic, IDictionary<string, byte[]> headers, bool isValue = true);
    public bool IsOfType(string type)
    {
        return GetType().FullName == type;
    }
}
