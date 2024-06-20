using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kafka.Connect.Plugin.Extensions;

namespace Kafka.Connect.Plugin.Models;

public class ConnectMessage<T>
{
    public T Key { get; set; }
    public T Value { get; set; }
    public long Timestamp { get; set; }
    public IDictionary<string, T> Headers { get; set; }
}

public class ConnectMessage<TKey, TValue> 
{
    public TKey Key { get; set; }
    public TValue Value { get; set; }
    public IDictionary<string, byte[]> Headers { get; set; }
}

public static class ConnectMessageExtensions
{
    public static ConnectMessage<IDictionary<string, object>> Convert(this ConnectMessage<JsonNode> message)
    {
        return new ConnectMessage<IDictionary<string, object>>
        {
            Key = message.Key?.ToDictionary(),
            Value = message.Value?.ToDictionary(),
        };
    }
    
    public static T GetValue<T>(this ConnectMessage<JsonNode> message, params string[] keys)
    {
        var dictionary = message.Value.ToDictionary();
        foreach (var key in keys)
        {
            if (dictionary.TryGetValue(key, out var value))
                return (T)value;
        }

        return default;
    }
    
    
    public static ConnectMessage<TKey, TValue> Convert<TKey, TValue>(this ConnectMessage<JsonNode> message)
    {
        return new ConnectMessage<TKey, TValue>
        {
            Key = message.Key == null ? default : message.Key.GetValue<TKey>(),
            Value =  message.Value == null ? default : message.Value.GetValue<TValue>(),
        };
    }
    
    public static ConnectMessage<JsonNode> Convert<TKey, TValue>(this ConnectMessage<TKey, TValue> message)
    {
        return new ConnectMessage<JsonNode>
        {
            Key = message.Key == null ? JsonNode.Parse("{}") : JsonSerializer.SerializeToNode(message.Key),
            Value =  message.Value == null ? JsonNode.Parse("{}") : JsonSerializer.SerializeToNode(message.Value),
        };
    }
    
    public static ConnectMessage<JsonNode> Convert(this ConnectMessage<IDictionary<string, object>> message)
    {
        return new ConnectMessage<JsonNode>
        {
            Key = message.Key?.ToJson(),
            Value = message.Value?.ToJson(),
        };
    }
}