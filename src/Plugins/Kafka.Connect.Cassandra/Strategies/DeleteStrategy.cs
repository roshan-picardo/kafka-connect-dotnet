using Kafka.Connect.Cassandra.Models;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using System.Text.Json.Nodes;

namespace Kafka.Connect.Cassandra.Strategies;

public class DeleteStrategy(ILogger<DeleteStrategy> logger, IConfigurationProvider configurationProvider)
    : Strategy<string>
{
    protected override Task<StrategyModel<string>> BuildModels(string connector, ConnectRecord record)
    {
        using (logger.Track("Creating delete CQL"))
        {
            var config = configurationProvider.GetPluginConfig<PluginConfig>(connector);
            var payload = record.Deserialized.Value.ToDictionary();
            var where = BuildWhere(config.Filter, payload);
            var cql = $"DELETE FROM {config.Keyspace}.{config.Table} WHERE {where};";

            return Task.FromResult(new StrategyModel<string>
            {
                Key = record.Key,
                Status = Status.Deleting,
                Model = cql
            });
        }
    }

    protected override Task<StrategyModel<string>> BuildModels(string connector, CommandRecord record)
        => throw new NotImplementedException();

    private static string BuildWhere(string filter, IDictionary<string, object> values)
    {
        var where = filter;
        foreach (var kv in values)
        {
            where = where.Replace($"#{kv.Key}#", kv.Value?.ToString(), StringComparison.OrdinalIgnoreCase);
        }

        return where;
    }
}
