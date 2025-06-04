using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.SqlServer.Models;

namespace Kafka.Connect.SqlServer.Strategies;

public class InsertStrategy(ILogger<InsertStrategy> logger, IConfigurationProvider configurationProvider)
    : Strategy<string>
{
    protected override Task<StrategyModel<string>> BuildModels(string connector, ConnectRecord record)
    {
        using (logger.Track("Creating insert SQL"))
        {
            var config = configurationProvider.GetPluginConfig<PluginConfig>(connector);
            var value = record.Deserialized.Value.ToDictionary();
            var columns = string.Join(", ", value.Keys);
            var values = string.Join(", ", value.Keys.Select(k => $"@{k}"));
            var parameters = value.Select(kv => $"DECLARE @{kv.Key} NVARCHAR(MAX) = '{kv.Value}'").ToList();

            var sql = $"""
                       {string.Join(";\n", parameters)};
                       INSERT INTO [{config.Schema}].[{config.Table}] ({columns})
                       VALUES ({values});
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
