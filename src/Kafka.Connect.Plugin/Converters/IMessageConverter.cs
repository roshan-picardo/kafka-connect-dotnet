using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Kafka.Connect.Plugin.Converters;

public interface IMessageConverter
{
    Task<byte[]> Serialize(string topic, JsonNode data, string subject = null, IDictionary<string, byte[]> headers = null, bool isValue = true);
    Task<JsonNode> Deserialize(string topic, ReadOnlyMemory<byte> data,  IDictionary<string, byte[]> headers, bool isValue = true);
}
