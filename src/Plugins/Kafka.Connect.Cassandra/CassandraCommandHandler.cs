using System.Text.Json.Nodes;
using Kafka.Connect.Cassandra.Models;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;

namespace Kafka.Connect.Cassandra;

public interface ICassandraCommandHandler
{
    Task Initialize(string connector);
    IDictionary<string, Command> Get(string connector);
    JsonNode Next(CommandRecord command, IList<ConnectMessage<JsonNode>> records);
    Task Purge(string connector);
}

public class CassandraCommandHandler(
    IConfigurationProvider configurationProvider,
    ILogger<CassandraCommandHandler> logger)
    : ICassandraCommandHandler
{
    public Task Initialize(string connector)
    {
        logger.Debug($"Cassandra connector initialized: {connector}");
        return Task.CompletedTask;
    }

    public IDictionary<string, Command> Get(string connector)
    {
        var config = configurationProvider.GetPluginConfig<PluginConfig>(connector);
        return config.Commands.ToDictionary(k => k.Key, v => v.Value as Command);
    }

    public JsonNode Next(CommandRecord command, IList<ConnectMessage<JsonNode>> records)
    {
        var config = command.GetCommand<CommandConfig>();

        if (records.Count > 0 && config.Filters.Count > 0)
        {
            var last = records.LastOrDefault()?.Value?["after"] as JsonObject;
            if (last != null)
            {
                foreach (var key in config.Filters.Keys.ToList())
                {
                    if (last.TryGetPropertyValue(key, out var value))
                    {
                        config.Filters[key] = value?.GetValue<string>() ?? string.Empty;
                    }
                }
            }

            config.Snapshot.Id = records.Max(m => m.Value?["id"]?.GetValue<long>() ?? 0);
        }

        if (records.Count < command.BatchSize)
        {
            config.Snapshot.Enabled = false;
            config.Snapshot.Id = 0;
            foreach (var key in config.Filters.Keys.ToList())
            {
                config.Filters[key] = string.Empty;
            }
        }

        return config.ToJson();
    }

    public Task Purge(string connector)
    {
        logger.Debug($"Cassandra connector purge noop: {connector}");
        return Task.CompletedTask;
    }
}
