using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Postgres.Models;

namespace Kafka.Connect.Postgres.Strategies;

public class DeleteStrategy(ILogger<DeleteStrategy> logger, IConfigurationProvider configurationProvider)
    : QueryStrategy<string>
{
    protected override Task<StrategyModel<string>> BuildSinkModels(string connector, ConnectRecord record)
    {
        using (logger.Track("Building delete statement"))
        {
            var config = configurationProvider.GetPluginConfig<SinkConfig>(connector);
            var whereClause = "";
            if (config.Filter != null)
            {
                whereClause = string.Format(config.Filter.Condition,
                    config.Filter.Keys?.Select(key => record.Deserialized.Value[key]).ToArray() ??
                    Array.Empty<object>());
            }

            var deleteQuery = $"DELETE FROM {config.Schema}.{config.Table} WHERE {whereClause};";
            return Task.FromResult(new StrategyModel<string>
            {
                Status = SinkStatus.Deleting,
                Model = deleteQuery
            });
        }
    }

    protected override Task<StrategyModel<string>> BuildSourceModels(string connector, CommandRecord record)
    {
        throw new NotImplementedException();
    }
}
