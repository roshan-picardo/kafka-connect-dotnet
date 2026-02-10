using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using Kafka.Connect.DynamoDb.Models;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;

namespace Kafka.Connect.DynamoDb;

public interface IDynamoDbCommandHandler
{
    IDictionary<string, Command> Get(string connector);
    JsonNode Next(CommandRecord command, IList<ConnectMessage<JsonNode>> records);
}

public class DynamoDbCommandHandler(IConfigurationProvider configurationProvider)
    : IDynamoDbCommandHandler
{
    public IDictionary<string, Command> Get(string connector)
    {
        var config = configurationProvider.GetPluginConfig<PluginConfig>(connector);
        return config.Commands.ToDictionary(k => k.Key, v => v.Value as Command);
    }

    public JsonNode Next(CommandRecord command, IList<ConnectMessage<JsonNode>> records)
    {
        var config = command.GetCommand<CommandConfig>();
        
        if (!command.IsChangeLog())
        {
            if (records.Count > 0)
            {
                // Check if this is a stream (has sequence number)
                var lastRecord = records.LastOrDefault();
                if (lastRecord?.Value?["_sequenceNumber"] != null)
                {
                    // For streams, store the sequence number
                    config.Filters ??= new Dictionary<string, object>();
                    config.SequenceNumber = lastRecord.Value["_sequenceNumber"].ToString();
                    config.Timestamp = lastRecord.Timestamp;
                }
                else
                {
                    // For regular scans/queries, update filters
                    var sorted = records.Select(r => r.Value.ToDictionary()).OrderBy(_ => 1);
                    foreach (var key in config.Filters?.Keys ?? Enumerable.Empty<string>())
                    {
                        sorted = sorted.ThenBy(d => d.ContainsKey(key) ? d[key] : null);
                    }
                    
                    var lastItem = sorted.LastOrDefault();
                    if (lastItem != null && config.Filters != null)
                    {
                        config.Filters = lastItem.Where(x => config.Filters.ContainsKey(x.Key))
                            .ToDictionary();
                    }
                }
            }
        }
        else if (records.Any())
        {
            var maxTimestamp = records.Max(m => m.Timestamp);
            var keys = records.Where(m => m.Timestamp == maxTimestamp).Select(m => m.Key.ToDictionary())
                .OrderBy(_ => 1);
            
            if (config.Keys != null)
            {
                keys = config.Keys.Aggregate(keys, (current, keyColumn) => 
                    current.ThenBy(d => d.ContainsKey(keyColumn) ? d[keyColumn] : null));
            }
            
            config.Timestamp = maxTimestamp;
            config.Filters = keys.LastOrDefault();
        }

        return config.ToJson();
    }
}
