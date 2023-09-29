using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.MongoDb.Collections;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Kafka.Connect.MongoDb;

public class MongoSinkHandler : SinkHandler<WriteModel<BsonDocument>>
{
    private readonly IMongoWriter _mongoWriter;

    public MongoSinkHandler(
        ILogger<SinkHandler<WriteModel<BsonDocument>>> logger,
        IWriteStrategyProvider writeStrategyProvider,
        IConfigurationProvider configurationProvider, 
        IMongoWriter mongoWriter) : base(logger, writeStrategyProvider, configurationProvider)
    {
        _mongoWriter = mongoWriter;
    }

    protected override async Task Sink(string connector, int taskId, BlockingCollection<SinkRecord<WriteModel<BsonDocument>>> sinkBatch)
    {
        await _mongoWriter.WriteMany(
            sinkBatch.Where(s => s.Ready)
                .OrderBy(s => s.Topic)
                .ThenBy(s => s.Partition)
                .ThenBy(s => s.Offset).ToList(),
            connector); //lets preserve the order
    }
}
