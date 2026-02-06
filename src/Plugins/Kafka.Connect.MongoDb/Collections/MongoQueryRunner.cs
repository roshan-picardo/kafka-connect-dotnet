using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.MongoDb.Models;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using MongoDB.Bson;
using MongoDB.Driver;

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

        public async Task<(IList<ChangeStreamDocument<BsonDocument>> Documents, string ResumeToken)> ReadChangeStream(
            StrategyModel<ChangeStreamModel<BsonDocument>> model,
            string connector,
            int taskId,
            string collection,
            int batchSize)
        {
            using (logger.Track("Reading from change stream"))
            {
                var sourceConfig = configurationProvider.GetPluginConfig<PluginConfig>(connector);
                var dbCollection = mongoClientProvider.GetMongoClient(connector, taskId)
                    .GetDatabase(sourceConfig.Database)
                    .GetCollection<BsonDocument>(collection);

                using (ConnectLog.Mongo(sourceConfig.Database, collection))
                {
                    try
                    {
                        var documents = new List<ChangeStreamDocument<BsonDocument>>();
                        string lastResumeToken = null;

                        // Watch the collection for changes
                        using (var cursor = await dbCollection.WatchAsync(
                            model.Model.Pipeline,
                            model.Model.Options))
                        {
                            // Try to get documents up to batch size with timeout
                            var startTime = DateTime.UtcNow;
                            var maxWaitTime = TimeSpan.FromMilliseconds(model.Model.MaxAwaitTimeMs);

                            while (documents.Count < batchSize &&
                                   (DateTime.UtcNow - startTime) < maxWaitTime)
                            {
                                if (await cursor.MoveNextAsync())
                                {
                                    foreach (var change in cursor.Current)
                                    {
                                        documents.Add(change);
                                        
                                        // Store the resume token from the last document
                                        if (change.ResumeToken != null)
                                        {
                                            lastResumeToken = change.ResumeToken.ToJson();
                                        }

                                        if (documents.Count >= batchSize)
                                        {
                                            break;
                                        }
                                    }
                                }
                                else
                                {
                                    // No more changes available, break out
                                    break;
                                }
                            }
                        }

                        logger.Debug("Change stream read completed.", new
                        {
                            DocumentsRead = documents.Count,
                            HasResumeToken = !string.IsNullOrEmpty(lastResumeToken)
                        });

                        return (documents, lastResumeToken);
                    }
                    catch (MongoCommandException ex) when (ex.CodeName == "ChangeStreamHistoryLost")
                    {
                        logger.Warning("Change stream history lost. Consider starting from latest.", ex);
                        throw new ConnectRetriableException("Change stream history lost", ex);
                    }
                    catch (MongoCommandException ex) when (ex.Code == 40573 || ex.Message.Contains("The $changeStream stage is only supported on replica sets"))
                    {
                        logger.Error("Change Streams require a MongoDB replica set or sharded cluster. Standalone instances are not supported.", ex);
                        throw new ConnectDataException(
                            "Change Streams are not supported on standalone MongoDB instances. " +
                            "Please either: 1) Use a replica set or sharded cluster, or 2) Set 'useChangeStreams: false' in your configuration to use polling-based reads.",
                            ex);
                    }
                    catch (MongoException me)
                    {
                        logger.Error("Error reading from change stream", me);
                        throw new ConnectRetriableException(me.Message, me);
                    }
                }
            }
        }
    }
}