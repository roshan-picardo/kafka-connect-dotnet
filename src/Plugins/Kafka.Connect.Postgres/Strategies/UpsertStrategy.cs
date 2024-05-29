using System.Text;
using System.Text.Json;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Postgres.Models;

namespace Kafka.Connect.Postgres.Strategies;

public class UpsertStrategy(ILogger<UpsertStrategy> logger, IConfigurationProvider configurationProvider)
    : QueryStrategy<string>
{
    protected override Task<StrategyModel<string>> BuildSinkModels(string connector, ConnectRecord record)
    {
        using (logger.Track("Building upsert statements"))
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

            var selectQuery = $"SELECT 1 FROM {config.Schema}.{config.Table} WHERE {whereClause}";
            
            var updateQuery = new StringBuilder($"UPDATE {config.Schema}.{config.Table} SET ");
            updateQuery.Append($" ({fields}) = ");
            updateQuery.Append(
                $"(SELECT {fields} FROM json_populate_record(null::{config.Schema}.{config.Table}, '{record.Deserialized.Value}')) ");
            updateQuery.Append($"WHERE {whereClause};");

            var insertQuery = new StringBuilder($"INSERT INTO {config.Schema}.{config.Table} ");
            insertQuery.Append($" ({fields}) ");
            insertQuery.Append(
                $"SELECT {fields} FROM json_populate_record(null::{config.Schema}.{config.Table}, '{record.Deserialized.Value}') ");
            insertQuery.Append($"WHERE {whereClause};");

            var finalQuery = new StringBuilder($"DO $do$ BEGIN IF EXISTS ({selectQuery}) THEN ");
            finalQuery.Append(updateQuery);
            finalQuery.Append(" ELSE ");
            finalQuery.Append(insertQuery);
            finalQuery.Append(" END IF; END $do$");
            
            return Task.FromResult(new StrategyModel<string>
            {
                Status = SinkStatus.Updating,
                Model = finalQuery.ToString()
            });
        }
    }

    protected override Task<StrategyModel<string>> BuildSourceModels(string connector, CommandRecord record)
    {
        throw new NotImplementedException();
    }
}
