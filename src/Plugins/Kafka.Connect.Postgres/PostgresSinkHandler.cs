using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using Npgsql;
using Serilog.Context;
using Serilog.Core.Enrichers;

namespace Kafka.Connect.Postgres;

public class PostgresSinkHandler : SinkHandler
{
    private readonly ILogger<SinkHandler> _logger;
    private readonly IPostgresClientProvider _postgresClientProvider;

    public PostgresSinkHandler(
        ILogger<SinkHandler> logger,
        IReadWriteStrategyProvider readWriteStrategyProvider,
        IConfigurationProvider configurationProvider,
        IPostgresClientProvider postgresClientProvider) : base(logger, readWriteStrategyProvider, configurationProvider)
    {
        _logger = logger;
        _postgresClientProvider = postgresClientProvider;
    }

    public override async Task Put(IEnumerable<ConnectRecord> records, string connector, int taskId)
    {
        using (_logger.Track("Putting batch of records"))
        {
            var models = new List<StrategyModel<string>>();
            await records.ForEachAsync(10, async cr =>
            {
                using (LogContext.Push(new PropertyEnricher("topic", cr.Topic),
                           new PropertyEnricher("partition", cr.Partition), new PropertyEnricher("offset", cr.Offset)))
                {
                    if (cr is not ConnectRecord record ||
                        record.Status is SinkStatus.Updated or SinkStatus.Deleted or SinkStatus.Inserted
                            or SinkStatus.Skipped)
                        return;
                    if (!record.Skip)
                    {
                        var model = await GetReadWriteStrategy(connector, record)
                            .Build<string>(connector, record);
                        models.Add(model);
                        record.Status = model.Status;
                    }
                    else
                    {
                        record.Status = SinkStatus.Skipping;
                    }
                }
            });


            foreach (var model in models.OrderBy(m => m.Topic)
                         .ThenBy(m => m.Partition)
                         .ThenBy(m => m.Offset)
                         .SelectMany(m => m.Models))
            {
                var command = new NpgsqlCommand(model,
                    _postgresClientProvider.GetPostgresClient(connector, taskId).GetConnection());
                await command.ExecuteNonQueryAsync();
            }
        }
    }
}