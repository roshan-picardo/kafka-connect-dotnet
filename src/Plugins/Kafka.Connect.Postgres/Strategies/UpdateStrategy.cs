using System.Text;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Postgres.Models;

namespace Kafka.Connect.Postgres.Strategies;

public class UpdateStrategy : WriteStrategy<string>
{
    private readonly ILogger<UpdateStrategy> _logger;
    private readonly IConfigurationProvider _configurationProvider;

    public UpdateStrategy(ILogger<UpdateStrategy> logger, IConfigurationProvider configurationProvider)
    {
        _logger = logger;
        _configurationProvider = configurationProvider;
    }
    protected override async Task<(SinkStatus Status, IList<string> Models)> BuildModels(string connector, SinkRecord record)
    {
        using (_logger.Track("Building update statement"))
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

            var updateQuery = new StringBuilder($"UPDATE {config.Schema}.{config.Table} SET ");
            updateQuery.Append($" ({fields}) = ");
            updateQuery.Append(
                $"(SELECT {fields} FROM json_populate_record(null::{config.Schema}.{config.Table}, '{record.Value}')) ");
            updateQuery.Append($"WHERE {whereClause};");
            
            return await Task.FromResult((SinkStatus.Updating, new[] { updateQuery.ToString() }));
        }
    }
}
