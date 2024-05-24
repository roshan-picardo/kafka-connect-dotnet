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

public class MongoSinkHandler(
    ILogger<SinkHandler> logger,
    IReadWriteStrategyProvider readWriteStrategyProvider,
    IConfigurationProvider configurationProvider,
    IMongoQueryRunner mongoQueryRunner)
    : SinkHandler(logger, readWriteStrategyProvider, configurationProvider)
{
    private readonly ILogger<SinkHandler> _logger = logger;

    public override async Task Put(IEnumerable<ConnectRecord> records, string connector, int taskId)
    {
        using (_logger.Track("Putting batch of records"))
        {
            var models = new List<StrategyModel<WriteModel<BsonDocument>>>();
            await records.ForEachAsync(10, async cr =>
            {
                using (ConnectLog.TopicPartitionOffset(cr.Topic, cr.Partition, cr.Offset))
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

            await mongoQueryRunner.WriteMany(models.OrderBy(m => m.Topic)
                .ThenBy(m => m.Partition)
                .ThenBy(m => m.Offset)
                .SelectMany(m => m.Models).ToList(), connector, taskId);
        }
    }
}
