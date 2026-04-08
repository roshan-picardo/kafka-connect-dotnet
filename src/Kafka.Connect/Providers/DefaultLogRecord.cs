using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;

namespace Kafka.Connect.Providers;

public class DefaultLogRecord(Kafka.Connect.Plugin.Providers.IConfigurationProvider configurationProvider)
    : ILogRecord
{
    public object Enrich(ConnectRecord record, string connector)
    {
        var attributes = configurationProvider.GetLogAttributes<string[]>(connector);
        var value = record.Deserialized.Value?.ToDictionary() ?? new Dictionary<string, object>();
        var result = new Dictionary<string, object>();
        
        foreach (var attr in attributes)
        {
            if (attr == "_key")
            {
                result["_key"] = record.Deserialized.Key.ToString();
                continue;
            }
            
            if (attr.EndsWith("[*]"))
            {
                var prefix = attr[..^3]; 
                var arrayItems = value.Where(kvp => kvp.Key.StartsWith($"{prefix}[") && kvp.Value != null)
                    .OrderBy(kvp => kvp.Key)
                    .Select(kvp => kvp.Value.ToString())
                    .ToList();
                
                if (arrayItems.Count != 0)
                {
                    result[prefix] = arrayItems;
                }
            }
            else if (value.TryGetValue(attr, out var exactValue) && exactValue != null)
            {
                result[attr] = exactValue;
            }
        }
        
        return result.ToNested();
    }
}