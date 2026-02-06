using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Web;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.MongoDb.Models;

public class PluginConfig
{
    private string _connectionUri;

    public string ConnectionUri
    {
        get => _connectionUri == null
            ? null
            : string.Format(_connectionUri, Username,
                Password == null
                    ? Password
                    : HttpUtility.UrlEncode(Encoding.UTF8.GetString(Convert.FromBase64String(Password))),
                Database);
        set => _connectionUri = value;
    }

    public string Database { get; set; }
    public string Password { get; set; }
    public string Username { get; set; }
    
    public string Collection { get; set; }
    public bool IsWriteOrdered { get; set; } = true;
    public string Filter { get; set; }
    
    public bool UseChangeStreams { get; set; } = false;
    public int ChangeStreamMaxAwaitTimeMs { get; set; } = 1000;
    public string ChangeStreamStartMode { get; set; } = "latest"; // "latest", "earliest", "timestamp"
    public long? ChangeStreamStartTimestamp { get; set; }
    
    public IDictionary<string, CommandConfig> Commands { get; set; }
}



public class CommandConfig : Command
{
    public long Timestamp { get; set; }
    public IDictionary<string, object> Filters { get; set; }
    public string Collection { get; set; }
    public string TimestampColumn { get; set; }
    public string[] Keys { get; set; }
    public string ResumeToken { get; set; }  // For Change Streams resume capability
        
    public override JsonNode ToJson() => JsonSerializer.SerializeToNode(this);
}