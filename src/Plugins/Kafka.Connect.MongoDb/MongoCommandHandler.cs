using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using Kafka.Connect.MongoDb.Models;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;

namespace Kafka.Connect.MongoDb;

public interface IMongoCommandHandler
{
    IDictionary<string, Command> Get(string connector);
    JsonNode Next(CommandRecord command, IList<ConnectMessage<JsonNode>> records);
}

public class MongoCommandHandler(IConfigurationProvider configurationProvider)
    : IMongoCommandHandler
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
                var sorted = records.Select(r => r.Value["after"].ToDictionary("after", true)).OrderBy(_ => 1);
                foreach (var key in config.Filters.Keys)
                {
                    sorted = sorted.ThenBy(d => d[key]);
                }
                config.Filters = sorted.LastOrDefault()?.Where(x => config.Filters.ContainsKey(x.Key))
                    .ToDictionary();
            }
        }
        else if (records.Any())
        {
            var maxTimestamp = records.Max(m => m.Timestamp);
            var keys = records.Where(m => m.Timestamp == maxTimestamp).Select(m => m.Key.ToDictionary())
                .OrderBy(_ => 1);
            keys = config.Keys.Aggregate(keys, (current, keyColumn) => current.ThenBy(d => d[keyColumn]));
            config.Timestamp = maxTimestamp;
            config.Filters = keys.LastOrDefault();
        }

        return config.ToJson();
    }
}
