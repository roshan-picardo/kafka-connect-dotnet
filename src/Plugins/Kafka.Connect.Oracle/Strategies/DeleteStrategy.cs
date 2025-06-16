using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Oracle.Models;

namespace Kafka.Connect.Oracle.Strategies;

public class DeleteStrategy(ILogger<DeleteStrategy> logger, IConfigurationProvider configurationProvider)
    : Strategy<string>
{
    protected override Task<StrategyModel<string>> BuildModels(string connector, ConnectRecord record)
    {
        using (logger.Track("Creating delete SQL"))
        {
            var config = configurationProvider.GetPluginConfig<PluginConfig>(connector);
            var deserialized = record.Deserialized.Value.ToDictionary();
            var lookupClause = $"{BuildCondition(config.Lookup ?? config.Filter, deserialized)}";

            var sql = $"""
                       DELETE FROM "{config.Schema}"."{config.Table}"
                       WHERE {lookupClause}
                       """;
            
            return Task.FromResult(new StrategyModel<string>
            {
                Key = record.Key,
                Status = Status.Deleting,
                Model = sql
            });
        }
    }

    protected override Task<StrategyModel<string>> BuildModels(string connector, CommandRecord record)
        => throw new NotImplementedException();
}
