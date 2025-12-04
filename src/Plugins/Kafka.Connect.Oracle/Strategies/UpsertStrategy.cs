using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Oracle.Models;

namespace Kafka.Connect.Oracle.Strategies;

public class UpsertStrategy(ILogger<UpsertStrategy> logger, IConfigurationProvider configurationProvider)
    : Strategy<string>
{
    protected override Task<StrategyModel<string>> BuildModels(string connector, ConnectRecord record)
    {
        using (logger.Track("Creating upsert SQL (merge)"))
        {
            var config = configurationProvider.GetPluginConfig<PluginConfig>(connector);
            var deserialized = record.Deserialized.Value.ToDictionary();
            var lookupClause = $"{BuildCondition(config.Lookup ?? config.Filter, deserialized)}";
            var lookupParams = GetConditionParameters(config.Lookup ?? config.Filter);
            var updates = string.Join(", ",
                deserialized.Where(d => !lookupParams.Contains(d.Key)).Select(d => $"\"{d.Key}\" = {GetValueByType(d.Value)}"));
            var fields = string.Join(',', deserialized.Keys.Select(k => $"\"{k}\""));
            var inserts = string.Join(", ",
                deserialized.Select(d => GetValueByType(d.Value)));

            var sql = $"""
                      MERGE INTO {config.Schema}.{config.Table}
                      USING DUAL
                        ON ({lookupClause})
                      WHEN MATCHED THEN
                        UPDATE SET 
                          {updates}
                      WHEN NOT  MATCHED THEN
                        INSERT ({fields})
                        VALUES({inserts})
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
