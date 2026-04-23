using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Db2.Models;

namespace Kafka.Connect.Db2.Strategies;

public class DeleteStrategy(ILogger<DeleteStrategy> logger, IConfigurationProvider configurationProvider)
    : Strategy<string>
{
    protected override Task<StrategyModel<string>> BuildModels(string connector, ConnectRecord record)
    {
        using (logger.Track("Building delete statement"))
        {
            var config = configurationProvider.GetPluginConfig<PluginConfig>(connector);
            var value = record.Deserialized.Value.ToDictionary();
            var whereClause = BuildCondition(config.Filter, value);

            return Task.FromResult(new StrategyModel<string>
            {
                Status = Status.Deleting,
                Model = $"""
                         DELETE FROM {config.Schema}.{config.Table}
                         WHERE {whereClause}
                         """
            });
        }
    }

    protected override Task<StrategyModel<string>> BuildModels(string connector, CommandRecord record)
        => throw new NotImplementedException();
}
