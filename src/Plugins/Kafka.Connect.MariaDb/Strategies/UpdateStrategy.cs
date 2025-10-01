using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.MariaDb.Models;

namespace Kafka.Connect.MariaDb.Strategies;

public class UpdateStrategy(ILogger<UpdateStrategy> logger, IConfigurationProvider configurationProvider)
    : Strategy<string>
{
    protected override Task<StrategyModel<string>> BuildModels(string connector, ConnectRecord record)
    {
        using (logger.Track("Creating update SQL"))
        {
            var config = configurationProvider.GetPluginConfig<PluginConfig>(connector);
            var deserialized = record.Deserialized.Value.ToDictionary();
            var setValues = string.Join(", ", deserialized.Select(d =>
                $"`{d.Key}` = {GetValueByType(d.Value)}"));
            
            var sql = $"""
                       UPDATE `{config.Schema}`.`{config.Table}`
                       SET {setValues}
                       WHERE {BuildCondition(config.Filter, deserialized)};
                       """;
            return Task.FromResult(new StrategyModel<string>
            {
                Key = record.Key,
                Status = Status.Updating,
                Model = sql
            });
        }
    }

    protected override Task<StrategyModel<string>> BuildModels(string connector, CommandRecord record)
        => throw new NotImplementedException();
    
}
