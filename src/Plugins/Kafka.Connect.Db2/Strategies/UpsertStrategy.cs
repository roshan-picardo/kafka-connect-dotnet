using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Db2.Models;

namespace Kafka.Connect.Db2.Strategies;

public class UpsertStrategy(ILogger<UpsertStrategy> logger, IConfigurationProvider configurationProvider)
    : Strategy<string>
{
    protected override Task<StrategyModel<string>> BuildModels(string connector, ConnectRecord record)
    {
        using (logger.Track("Building merge statement"))
        {
            var config = configurationProvider.GetPluginConfig<PluginConfig>(connector);
            var value = record.Deserialized.Value.ToDictionary();
            var lookupFields = GetConditionParameters(config.Lookup ?? config.Filter);
            var columns = value.Keys.ToList();
            var onClause = string.Join(" AND ", lookupFields.Select(field => $"target.\"{field}\" = {GetValueByType(value[field])}"));
            var setClause = string.Join(", ", value.Select(kv => $"target.\"{kv.Key}\" = {GetValueByType(kv.Value)}"));
            var insertColumns = string.Join(", ", columns.Select(column => $"\"{column}\""));
            var insertValues = string.Join(", ", columns.Select(column => GetValueByType(value[column])));

            return Task.FromResult(new StrategyModel<string>
            {
                Status = Status.Updating,
                Model = $"""
                         MERGE INTO {config.Schema}.{config.Table} AS target
                         USING SYSIBM.SYSDUMMY1 AS source
                         ON {onClause}
                         WHEN MATCHED THEN
                             UPDATE SET {setClause}
                         WHEN NOT MATCHED THEN
                             INSERT ({insertColumns})
                             VALUES ({insertValues})
                         """
            });
        }
    }

    protected override Task<StrategyModel<string>> BuildModels(string connector, CommandRecord record)
        => throw new NotImplementedException();
}
