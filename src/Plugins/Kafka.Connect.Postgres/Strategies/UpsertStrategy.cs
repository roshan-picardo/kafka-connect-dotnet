using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Postgres.Models;

namespace Kafka.Connect.Postgres.Strategies;

public class UpsertStrategy(ILogger<UpsertStrategy> logger, IConfigurationProvider configurationProvider)
    : Strategy<string>
{
    protected override Task<StrategyModel<string>> BuildModels(string connector, ConnectRecord record)
    {
        using (logger.Track("Building upsert statements"))
        {
            var config = configurationProvider.GetPluginConfig<PluginConfig>(connector);
            var deserialized = record.Deserialized.Value.ToDictionary();
            var lookupClause = $"WHERE {BuildCondition(config.Lookup ?? config.Filter, deserialized)}";
            var whereClause = $"WHERE {BuildCondition(config.Filter, deserialized)}";
            var fields = string.Join(',', deserialized.Keys);

            var upsertQuery = $"""
                               DO $do$
                                  BEGIN
                                      IF EXISTS (SELECT FROM {config.Schema}.{config.Table} {lookupClause}) THEN
                                          UPDATE {config.Schema}.{config.Table}
                                          SET ({fields}) = 
                                              (SELECT {fields} 
                                              FROM json_populate_record(null::{config.Schema}.{config.Table}, '{record.Deserialized.Value}'))
                                              {whereClause};
                                      ELSE
                                          INSERT INTO {config.Schema}.{config.Table} ({fields}) 
                                          SELECT {fields} 
                                          FROM json_populate_record(null::{config.Schema}.{config.Table}, '{record.Deserialized.Value}');
                                      END IF; 
                                  END
                               $do$;
                               """;
            
            return Task.FromResult(new StrategyModel<string>
            {
                Status = Status.Updating,
                Model = upsertQuery
            });
        }
    }

    protected override Task<StrategyModel<string>> BuildModels(string connector, CommandRecord record)
        => throw new NotImplementedException();
}
