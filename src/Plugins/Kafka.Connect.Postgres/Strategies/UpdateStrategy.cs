using System.Text;
using System.Text.Json;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Postgres.Models;

namespace Kafka.Connect.Postgres.Strategies;

public class UpdateStrategy(ILogger<UpdateStrategy> logger, IConfigurationProvider configurationProvider)
    : Strategy<string>
{
    protected override Task<StrategyModel<string>> BuildModels(string connector, ConnectRecord record)
    {
        using (logger.Track("Building update statement"))
        {
            var config = configurationProvider.GetPluginConfig<PluginConfig>(connector);
            var deserialized = record.Deserialized.Value.ToDictionary();
            var fields = string.Join(',', deserialized.Keys);
            
            return Task.FromResult(new StrategyModel<string>
            {
                Status = SinkStatus.Updating,
                Model = $"""
                         UPDATE {config.Schema}.{config.Table}
                         SET ({fields}) = 
                            (SELECT {fields}
                            FROM json_populate_record(null::{config.Schema}.{config.Table}, '{record.Deserialized.Value}'))
                            WHERE {BuildCondition(config.Filter, deserialized)};
                         """
            });
        }
    }

    protected override Task<StrategyModel<string>> BuildModels(string connector, CommandRecord record)
        => throw new NotImplementedException();
}
