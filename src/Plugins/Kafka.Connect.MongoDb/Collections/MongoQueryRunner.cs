using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.MongoDb.Models;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using MongoDB.Bson;
using MongoDB.Driver;
using Serilog.Context;
using Serilog.Core.Enrichers;

namespace Kafka.Connect.MongoDb.Collections
{
    public class MongoQueryRunner(
        ILogger<MongoQueryRunner> logger,
        IMongoClientProvider mongoClientProvider,
        IConfigurationProvider configurationProvider)
        : IMongoQueryRunner
    {
        public async Task WriteMany(IList<WriteModel<BsonDocument>> models, string connector, int taskId)
        {
            using (logger.Track("Writing models to database"))
            {
                var sinkConfig = configurationProvider.GetPluginConfig<PluginConfig>(connector);
                if (models == null || !models.Any())
                {
                    return;
                }

                using (ConnectLog.Mongo(sinkConfig.Database, sinkConfig.Collection))
                {
                    try
                    {
                        var collection = mongoClientProvider.GetMongoClient(connector, taskId)
                            .GetDatabase(sinkConfig.Database)
                            .GetCollection<BsonDocument>(sinkConfig.Collection);
                        var bulkWriteResult = await collection.BulkWriteAsync(models,
                            new BulkWriteOptions { IsOrdered = sinkConfig.IsWriteOrdered });
                        logger.Debug("Models written successfully to mongodb.",
                            new
                            {
                                Acknowledged = bulkWriteResult.IsAcknowledged,
                                Requests = bulkWriteResult.RequestCount,
                                Deleted = bulkWriteResult.DeletedCount,
                                Inserted = bulkWriteResult.InsertedCount,
                                Matched = bulkWriteResult.MatchedCount,
                                Modified = bulkWriteResult.ModifiedCount,
                                Processed = bulkWriteResult.ProcessedRequests.Count,
                                Upserts = bulkWriteResult.Upserts.Count,
                            });
                    }
                    catch (MongoBulkWriteException ex)
                    {
                        // TODO: set log context needs to be setup
                        throw new ConnectRetriableException(ex.Message, ex);
                    }
                    catch (MongoException me)
                    {
                        throw new ConnectRetriableException(me.Message, me);
                    }
                }
            }
        }

        public async Task<IList<BsonDocument>> ReadMany(
            StrategyModel<FindModel<BsonDocument>> model,
            string connector,
            int taskId,
            string collection)
        {
            var sourceConfig = configurationProvider.GetPluginConfig<PluginConfig>(connector);
            var dbCollection = mongoClientProvider.GetMongoClient(connector, taskId)
                .GetDatabase(sourceConfig.Database)
                .GetCollection<BsonDocument>(collection);

            return await (await dbCollection.FindAsync(model.Model.Filter, model.Model.Options)).ToListAsync();
        }

        public async Task<IList<JsonNode>> ReadMany(IList<ConnectRecord<(FilterDefinition<BsonDocument>, FindOptions<BsonDocument>)>> batch, string connector, int taskId)
        {
            var sourceConfig = configurationProvider.GetPluginConfig<PluginConfig>(connector);
            var collection = mongoClientProvider.GetMongoClient(connector, taskId)
                .GetDatabase(sourceConfig.Database)
                .GetCollection<BsonDocument>("mongoSourceConfig.Collection");

            var data = await (await collection.FindAsync(batch[0].Model.Item1, batch[0].Model.Item2)).ToListAsync();
            return data.Select(d => JsonNode.Parse(d.ToJson())).ToList();
        }
    }
}