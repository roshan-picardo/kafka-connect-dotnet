using System;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kafka.Connect.Plugin.Extensions;

namespace Kafka.Connect.Plugin.Models;

public class CommandRecord : IConnectRecord
{
    private Guid _id;
    public string Id
    {
        get
        {
            if (_id == Guid.Empty)
            {
                using var md5 = MD5.Create();
                _id = new Guid(md5.ComputeHash(Encoding.UTF8.GetBytes($"{Connector}-{Name}")));
            }
            return _id.ToString();
        }
    }

    public string Name { get; set; }
    public string Connector { get; set; }
    public string Topic { get; set; }
    public int Partition { get; set; }
    public long Offset { get; set; }
    public int BatchSize { get; set; }
    public JsonNode Command { get; set; }
    public Exception Exception { get; set; }

    public int GetVersion() => Command["Version"]?.GetValue<int>() ?? 0;
    public T GetCommand<T>() => Command.ToDictionary().ToJson().Deserialize<T>();
}
