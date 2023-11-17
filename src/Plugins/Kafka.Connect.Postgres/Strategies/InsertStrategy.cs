using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Postgres.Models;

namespace Kafka.Connect.Postgres.Strategies;

public class InsertStrategy : WriteStrategy<string>
{
    private readonly ILogger<InsertStrategy> _logger;
    private readonly IConfigurationProvider _configurationProvider;

    public InsertStrategy(ILogger<InsertStrategy> logger, IConfigurationProvider configurationProvider)
    {
        _logger = logger;
        _configurationProvider = configurationProvider;
    }

    protected override async Task<(SinkStatus Status, IList<string> Models)> BuildModels(string connector, ConnectRecord record)
    {
        using (_logger.Track("Building insert statement"))
        {
            var config = _configurationProvider.GetSinkConfigProperties<PostgresSinkConfig>(connector);
            var insertQuery =
                $"INSERT INTO {config.Schema}.{config.Table} SELECT * FROM json_populate_record(null::{config.Schema}.{config.Table}, '{record.DeserializedToken.Value}');";
            return await Task.FromResult((SinkStatus.Inserting, new[] { insertQuery }));
        }
    }
}