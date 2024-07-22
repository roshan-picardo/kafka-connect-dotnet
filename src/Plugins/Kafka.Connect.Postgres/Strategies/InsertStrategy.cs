using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Postgres.Models;

namespace Kafka.Connect.Postgres.Strategies;

public class InsertStrategy(ILogger<InsertStrategy> logger, IConfigurationProvider configurationProvider)
    : Strategy<string>
{
    protected override Task<StrategyModel<string>> BuildModels(string connector, ConnectRecord record)
    {
        using (logger.Track("Building insert statement"))
        {
            var config = configurationProvider.GetPluginConfig<PluginConfig>(connector);
            return Task.FromResult(new StrategyModel<string>
            {
                Status = SinkStatus.Inserting,
                Model = $"""
                         INSERT INTO {config.Schema}.{config.Table} 
                         SELECT * 
                         FROM json_populate_record(null::{config.Schema}.{config.Table}, '{record.Deserialized.Value}');
                         """
            });
        }
    }

    protected override Task<StrategyModel<string>> BuildModels(string connector, CommandRecord record)
        => throw new NotImplementedException();
}