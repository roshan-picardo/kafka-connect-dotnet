using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.Models;

public class SourceRecord : ConnectRecord
{
    public SourceRecord(
        string topic,
        JsonNode key,
        JsonNode value,
        bool skip = false) : base(topic, -1, -1)
    {
        Deserialized = new ConnectMessage<JsonNode>
        {
            Key = key,
            Value = value
        };
        Skip = skip;
        StartTiming();
    }

    public IDictionary<string, object> Keys =>
        Deserialized.Key[nameof(Keys)]?.ToDictionary(nameof(Keys), true)  ?? new Dictionary<string, object>();
}
