using System.Text.Json.Nodes;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.SqlServer.Models;

namespace Kafka.Connect.SqlServer.Strategies;

public class UpsertStrategy(ILogger<UpsertStrategy> logger, IConfigurationProvider configurationProvider)
    : Strategy<string>
{
    protected override Task<StrategyModel<string>> BuildModels(string connector, ConnectRecord record)
    {
        using (logger.Track("Creating upsert SQL"))
        {
            var config = configurationProvider.GetPluginConfig<PluginConfig>(connector);
            var value = record.Deserialized.Value.ToDictionary();
            var columns = string.Join(", ", value.Keys);
            var values = string.Join(", ", value.Keys.Select(k => $"@{k}"));
            var setClause = string.Join(", ", value.Keys.Select(k => $"{k} = @{k}"));
            var whereClause = BuildCondition(config.Lookup, value);
            var parameters = value.Select(kv => $"DECLARE @{kv.Key} NVARCHAR(MAX) = '{kv.Value}'").ToList();
            
            var sql = $"""
                       {string.Join(";\n", parameters)};
                       IF EXISTS (SELECT 1 FROM [{config.Schema}].[{config.Table}] WHERE {whereClause})
                       BEGIN
                           UPDATE [{config.Schema}].[{config.Table}]
                           SET {setClause}
                           WHERE {whereClause}
                       END
                       ELSE
                       BEGIN
                           INSERT INTO [{config.Schema}].[{config.Table}] ({columns})
                           VALUES ({values})
                       END;
                       """;
            return Task.FromResult(new StrategyModel<string>
            {
                Status = Status.Updating,
                Model = sql
            });
        }
    }

    protected override Task<StrategyModel<string>> BuildModels(string connector, CommandRecord record)
        => throw new NotImplementedException();
}
