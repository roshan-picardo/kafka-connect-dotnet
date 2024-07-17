using System;
using System.Text.Json;
using System.Text.Json.Nodes;
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

    public string Name { get; set; }
    public string Connector { get; set; }
    public string Topic { get; set; }
    public int Partition { get; set; }
    public long Offset { get; set; }
    public int BatchSize { get; set; }
    public JsonNode Changelog { get; set; }
    public JsonNode Command { get; set; }
    public Exception Exception { get; set; }

    public int GetVersion()
    {
        var hash = Command["Version"]?.GetValue<int>() ?? 0;
        if (hash == 0)
        {
            hash = Command.ToJsonString().ToGuid().GetHashCode() & 0x7FFFFFFF;
            Command["Version"] = hash;
        }
        return hash;
    }
    public T GetCommand<T>() => Get<T>(Command);
    public T GetChangelog<T>() => Get<T>(Changelog);
    
    public bool IsChangeLog() => Changelog != null;

    private static T Get<T>(JsonNode node) => node.ToDictionary().ToJson().Deserialize<T>();
}
