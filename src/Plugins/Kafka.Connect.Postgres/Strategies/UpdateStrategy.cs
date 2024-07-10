using System.Text;
using System.Text.Json;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Postgres.Models;

namespace Kafka.Connect.Postgres.Strategies;

public class UpdateStrategy(ILogger<UpdateStrategy> logger, IConfigurationProvider configurationProvider)
    : Strategy<string>
{
    protected override Task<StrategyModel<string>> BuildSinkModels(string connector, ConnectRecord record)
    {
        using (logger.Track("Building update statement"))
        {
            var config = configurationProvider.GetPluginConfig<SinkConfig>(connector);
            var whereClause = "";
            if (config.Filter != null)
            {
                whereClause = string.Format(config.Filter.Condition,
                    config.Filter.Keys?.Select(key => record.Deserialized.Value[key]).ToArray() ?? Array.Empty<object>());
            }

            var fields = string.Join(',',
                record.Deserialized.Value.Deserialize<IDictionary<string, object>>().Select(k => $"\"{k.Key}\""));

            var updateQuery = new StringBuilder($"UPDATE {config.Schema}.{config.Table} SET ");
            updateQuery.Append($" ({fields}) = ");
            updateQuery.Append(
                $"(SELECT {fields} FROM json_populate_record(null::{config.Schema}.{config.Table}, '{record.Deserialized.Value}')) ");
            updateQuery.Append($"WHERE {whereClause};");
            
            return Task.FromResult(new StrategyModel<string>
            {
                Status = SinkStatus.Updating,
                Model = updateQuery.ToString()
            });
        }
    }

    protected override Task<StrategyModel<string>> BuildSourceModels(string connector, CommandRecord record)
    {
        throw new NotImplementedException();
    }
}
