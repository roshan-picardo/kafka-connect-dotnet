using System;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Kafka.Connect.Plugin.Extensions;

namespace Kafka.Connect.Plugin.Models;

public class CommandRecord : IConnectRecord
{
    private Guid _id;
    public Guid Id
    {
        get
        {
            if (_id == Guid.Empty)
            {
                _id = $"{Connector}-{Name}".ToGuid();
            }
            return _id;
        }
    }

    public string Name { get; init; }
    public string Connector { get; init; }
    public string Topic { get; set; }
    public int Partition { get; set; }
    public long Offset { get; set; }
    public Guid Key => Id;
    public Status Status { get; set; }

    public int BatchSize { get; init; }
    public JsonNode Changelog { get; set; }
    public JsonNode Command { get; set; }
    [JsonIgnore]
    public Exception Exception { get; set; }
    
    public int GetVersion()
    {
        var hash = Command["Version"]?.GetValue<int>() ?? 0;
        if (hash != 0) return hash;
        hash = Command.ToJsonString().ToGuid().GetHashCode() & 0x7FFFFFFF;
        Command["Version"] = hash;
        return hash;
    }
    public T GetCommand<T>() => Get<T>(Command);
    public T GetChangelog<T>() => Get<T>(Changelog);
    
    public bool IsChangeLog() => Changelog != null;

    private static T Get<T>(JsonNode node) => node.ToDictionary().ToJson().Deserialize<T>();
}
