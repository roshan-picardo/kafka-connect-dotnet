using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.DynamoDb.Models;

public class PluginConfig
{
    public string Region { get; set; }
    public string AccessKeyId { get; set; }
    public string SecretAccessKey { get; set; }
    public string ServiceUrl { get; set; }
    public string TableName { get; set; }
    public bool IsWriteOrdered { get; set; } = true;
    public string Filter { get; set; }
    
    public IDictionary<string, CommandConfig> Commands { get; set; }
}

public class CommandConfig : Command
{
    public long Timestamp { get; set; }
    public IDictionary<string, object> Filters { get; set; }
    public string TableName { get; set; }
    public string TimestampColumn { get; set; }
    public string[] Keys { get; set; }
    public string IndexName { get; set; }
    public bool UseStreams { get; set; }
    public string StreamArn { get; set; }
    public string ShardIteratorType { get; set; }
    public string SequenceNumber { get; set; }
        
    public override JsonNode ToJson() => JsonSerializer.SerializeToNode(this);
}
