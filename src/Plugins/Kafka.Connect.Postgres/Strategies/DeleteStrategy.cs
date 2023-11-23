using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Postgres.Models;

namespace Kafka.Connect.Postgres.Strategies;

public class DeleteStrategy : WriteStrategy<string>
{
    private readonly ILogger<DeleteStrategy> _logger;
    private readonly IConfigurationProvider _configurationProvider;

    public DeleteStrategy(ILogger<DeleteStrategy> logger, IConfigurationProvider configurationProvider)
    {
        _logger = logger;
        _configurationProvider = configurationProvider;
    }

    protected override async Task<(SinkStatus Status, IList<string> Models)> BuildModels(string connector, ConnectRecord record)
    {
        using (_logger.Track("Building delete statement"))
        {
            var config = _configurationProvider.GetSinkConfigProperties<PostgresSinkConfig>(connector);
            var whereClause = "";
            if (config.Filter != null)
            {
                whereClause = string.Format(config.Filter.Condition,
                    config.Filter.Keys?.Select(key => record.Deserialized.Value[key]).ToArray() ?? Array.Empty<object>());
            }
            var deleteQuery = $"DELETE FROM {config.Schema}.{config.Table} WHERE {whereClause};";
            return await Task.FromResult((SinkStatus.Inserting, new[] { deleteQuery }));
        }
    }
}
