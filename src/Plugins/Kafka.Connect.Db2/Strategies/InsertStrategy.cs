using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Db2.Models;

namespace Kafka.Connect.Db2.Strategies;

public class InsertStrategy(ILogger<InsertStrategy> logger, IConfigurationProvider configurationProvider)
    : Strategy<string>
{
    protected override Task<StrategyModel<string>> BuildModels(string connector, ConnectRecord record)
    {
        using (logger.Track("Building insert statement"))
        {
            var config = configurationProvider.GetPluginConfig<PluginConfig>(connector);
            var value = record.Deserialized.Value.ToDictionary();
            var columns = string.Join(", ", value.Keys.Select(column => $"\"{column}\""));
            var values = string.Join(", ", value.Values.Select(GetValueByType));

            return Task.FromResult(new StrategyModel<string>
            {
                Key = record.Key,
                Status = Status.Inserting,
                Model = $"""
                         INSERT INTO {config.Schema}.{config.Table} ({columns})
                         VALUES ({values})
                         """
            });
        }
    }

    protected override Task<StrategyModel<string>> BuildModels(string connector, CommandRecord record)
        => throw new NotImplementedException();
}
