using System.Text;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Postgres.Models;

namespace Kafka.Connect.Postgres.Strategies;

public class UpsertStrategy : WriteStrategy<string>
{
    private readonly ILogger<UpsertStrategy> _logger;
    private readonly IConfigurationProvider _configurationProvider;

    public UpsertStrategy(ILogger<UpsertStrategy> logger, IConfigurationProvider configurationProvider)
    {
        _logger = logger;
        _configurationProvider = configurationProvider;
    }
    protected override async Task<(SinkStatus Status, IList<string> Models)> BuildModels(string connector, ConnectRecord record)
    {
        using (_logger.Track("Building upsert statements"))
        {
            var config = _configurationProvider.GetSinkConfigProperties<PostgresSinkConfig>(connector);
            var whereClause = "";
            if (config.Filter != null)
            {
                whereClause = string.Format(config.Filter.Condition,
                    config.Filter.Keys?.Select(key => record.Value.Value<object>(key)).ToArray() ?? Array.Empty<object>());
            }

            var fields = string.Join(',',
                record.Value.ToObject<IDictionary<string, object>>().Select(k => $"\"{k.Key}\""));

            var selectQuery = $"SELECT 1 FROM {config.Schema}.{config.Table} WHERE {whereClause}";
            
            var updateQuery = new StringBuilder($"UPDATE {config.Schema}.{config.Table} SET ");
            updateQuery.Append($" ({fields}) = ");
            updateQuery.Append(
                $"(SELECT {fields} FROM json_populate_record(null::{config.Schema}.{config.Table}, '{record.Value}')) ");
            updateQuery.Append($"WHERE {whereClause};");

            var insertQuery = new StringBuilder($"INSERT INTO {config.Schema}.{config.Table} ");
            insertQuery.Append($" ({fields}) ");
            insertQuery.Append(
                $"SELECT {fields} FROM json_populate_record(null::{config.Schema}.{config.Table}, '{record.Value}') ");
            insertQuery.Append($"WHERE {whereClause};");

            var finalQuery = new StringBuilder($"DO $do$ BEGIN IF EXISTS ({selectQuery}) THEN ");
            finalQuery.Append(updateQuery);
            finalQuery.Append(" ELSE ");
            finalQuery.Append(insertQuery);
            finalQuery.Append(" END IF; END $do$");
            
            return await Task.FromResult((SinkStatus.Updating, new[] { finalQuery.ToString() }));
        }
    }
}
