using System.Collections.Concurrent;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Npgsql;

namespace Kafka.Connect.Postgres;

public class PostgresSinkHandler : SinkHandler<string>
{
    private readonly IPostgresClientProvider _postgresClientProvider;

    public PostgresSinkHandler(
        ILogger<SinkHandler<string>> logger,
        IWriteStrategyProvider writeStrategyProvider,
        IConfigurationProvider configurationProvider,
        IPostgresClientProvider postgresClientProvider) : base(logger, writeStrategyProvider, configurationProvider)
    {
        _postgresClientProvider = postgresClientProvider;
    }

    protected override async Task Sink(string connector, int taskId,  BlockingCollection<SinkRecord<string>> sinkBatch)
    {
        foreach (var record in sinkBatch)
        {
            foreach (var model in record.Models)
            {
                var command = new NpgsqlCommand(model, _postgresClientProvider.GetPostgresClient(connector, taskId).GetConnection());
                await command.ExecuteNonQueryAsync();
            }    
        }
    }
}