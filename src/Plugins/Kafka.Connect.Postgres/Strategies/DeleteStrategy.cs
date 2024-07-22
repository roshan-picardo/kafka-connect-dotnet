using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Postgres.Models;

namespace Kafka.Connect.Postgres.Strategies;

public class DeleteStrategy(ILogger<DeleteStrategy> logger, IConfigurationProvider configurationProvider)
    : Strategy<string>
{
    protected override Task<StrategyModel<string>> BuildModels(string connector, ConnectRecord record)
    {
        using (logger.Track("Building delete statement"))
        {
            var config = configurationProvider.GetPluginConfig<PluginConfig>(connector);
            var deserialized = record.Deserialized.Value.ToDictionary();

            return Task.FromResult(new StrategyModel<string>
            {
                Status = SinkStatus.Deleting,
                Model = $"""
                         DELETE FROM {config.Schema}.{config.Table} 
                         WHERE {BuildCondition(config.Filter, deserialized)};
                         """
            });
        }
    }

    protected override Task<StrategyModel<string>> BuildModels(string connector, CommandRecord record)
        => throw new NotImplementedException();
}
