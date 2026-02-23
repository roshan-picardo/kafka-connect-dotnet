using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.MariaDb.Models;

namespace Kafka.Connect.MariaDb.Strategies;

public class InsertStrategy(ILogger<InsertStrategy> logger, IConfigurationProvider configurationProvider)
    : Strategy<string>
{
    protected override Task<StrategyModel<string>> BuildModels(string connector, ConnectRecord record)
    {
        using (logger.Track("Creating insert SQL"))
        {
            var config = configurationProvider.GetPluginConfig<PluginConfig>(connector);
            var deserialized = record.Deserialized.Value.ToDictionary();
            var fields = string.Join(", ", deserialized.Keys.Select(k => $"`{k}`"));
            var inserts = string.Join(", ", deserialized.Select(d => GetValueByType(d.Value)));

            var sql = $"""
                       INSERT INTO `{config.Schema}`.`{config.Table}` ({fields})
                       VALUES ({inserts})
                       """;
            return Task.FromResult(new StrategyModel<string>
            {
                Key = record.Key,
                Status = Status.Inserting,
                Model = sql
            });
        }
    }

    protected override Task<StrategyModel<string>> BuildModels(string connector, CommandRecord record)
        => throw new NotImplementedException();

}
