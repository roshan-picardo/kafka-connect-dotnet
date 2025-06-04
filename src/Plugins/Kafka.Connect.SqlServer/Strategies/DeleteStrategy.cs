using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.SqlServer.Models;

namespace Kafka.Connect.SqlServer.Strategies;

public class DeleteStrategy(ILogger<DeleteStrategy> logger, IConfigurationProvider configurationProvider)
    : Strategy<string>
{
    protected override Task<StrategyModel<string>> BuildModels(string connector, ConnectRecord record)
    {
        using (logger.Track("Creating delete SQL"))
        {
            var config = configurationProvider.GetPluginConfig<PluginConfig>(connector);
            var value = record.Deserialized.Value.ToDictionary();
            var whereClause = BuildCondition(config.Filter, value);
            var parameters = value.Select(kv => $"DECLARE @{kv.Key} NVARCHAR(MAX) = '{kv.Value}'").ToList();

            var sql = $"""
                       {string.Join(";\n", parameters)};
                       DELETE FROM [{config.Schema}].[{config.Table}]
                       WHERE {whereClause};
                       """;
            return Task.FromResult(new StrategyModel<string>
            {
                Status = Status.Deleting,
                Model = sql
            });
        }
    }

    protected override Task<StrategyModel<string>> BuildModels(string connector, CommandRecord record)
        => throw new NotImplementedException();
}
