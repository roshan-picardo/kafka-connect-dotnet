using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.MongoDb.Collections;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using MongoDB.Bson;
using MongoDB.Driver;
using Serilog.Context;
using Serilog.Core.Enrichers;

namespace Kafka.Connect.MongoDb;

public class MongoSinkHandler : SinkHandler
{
    private readonly ILogger<SinkHandler> _logger;
    private readonly IMongoQueryRunner _mongoQueryRunner;

    public MongoSinkHandler(
        ILogger<SinkHandler> logger,
        IReadWriteStrategyProvider readWriteStrategyProvider,
        IConfigurationProvider configurationProvider, 
        IMongoQueryRunner mongoQueryRunner) : base(logger, readWriteStrategyProvider, configurationProvider)
    {
        _logger = logger;
        _mongoQueryRunner = mongoQueryRunner;
    }

    public override async Task Put(IEnumerable<ConnectRecord> records, string connector, int taskId)
    {
        using (_logger.Track("Putting batch of records"))
        {
            var models = new List<StrategyModel<WriteModel<BsonDocument>>>();
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
                            .Build<WriteModel<BsonDocument>>(connector, record);
                        models.Add(model);
                        record.Status = model.Status;
                    }
                    else
                    {
                        record.Status = SinkStatus.Skipping;
                    }
                }
            });

            await _mongoQueryRunner.WriteMany(models.OrderBy(m => m.Topic)
                .ThenBy(m => m.Partition)
                .ThenBy(m => m.Offset)
                .SelectMany(m => m.Models).ToList(), connector, taskId);
        }
    }
}
