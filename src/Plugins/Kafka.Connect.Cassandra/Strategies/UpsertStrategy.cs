using Kafka.Connect.Cassandra.Models;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;

namespace Kafka.Connect.Cassandra.Strategies;

public class UpsertStrategy(ILogger<UpsertStrategy> logger, IConfigurationProvider configurationProvider)
    : Strategy<string>
{
    protected override Task<StrategyModel<string>> BuildModels(string connector, ConnectRecord record)
    {
        using (logger.Track("Creating upsert CQL"))
        {
            var config = configurationProvider.GetPluginConfig<PluginConfig>(connector);
            var payload = record.Deserialized.Value.ToDictionary();

            var columns = string.Join(", ", payload.Keys.Select(k => $"\"{k}\""));
            var values = string.Join(", ", payload.Values.Select(GetValueByType));

            var cql = $"INSERT INTO {config.Keyspace}.{config.Table} ({columns}) VALUES ({values});";

            return Task.FromResult(new StrategyModel<string>
            {
                Key = record.Key,
                Status = Status.Updating,
                Model = cql
            });
        }
    }

    protected override Task<StrategyModel<string>> BuildModels(string connector, CommandRecord record)
        => throw new NotImplementedException();
}
