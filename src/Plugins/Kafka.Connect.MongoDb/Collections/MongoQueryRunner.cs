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
    public class MongoQueryRunner : IMongoQueryRunner
    {
        private readonly IMongoClientProvider _mongoClientProvider;
        private readonly IConfigurationProvider _configurationProvider;
        private readonly ILogger<MongoQueryRunner> _logger;

        public MongoQueryRunner(
            ILogger<MongoQueryRunner> logger,
            IMongoClientProvider mongoClientProvider,
            IConfigurationProvider configurationProvider)
        {
            _logger = logger;
            _mongoClientProvider = mongoClientProvider;
            _configurationProvider = configurationProvider;
        }

        public async Task WriteMany(IList<WriteModel<BsonDocument>> models, string connector, int taskId)
        {
            using (_logger.Track("Writing models to database"))
            {
                var mongoSinkConfig = _configurationProvider.GetSinkConfigProperties<MongoSinkConfig>(connector);
            if (models == null || !models.Any())
            {
                return;
            }
            using (ConnectLog.Mongo(mongoSinkConfig.Database, mongoSinkConfig.Collection))
            {
                try
                {
                    var collection = _mongoClientProvider.GetMongoClient(connector, taskId)
                        .GetDatabase(mongoSinkConfig.Database)
                        .GetCollection<BsonDocument>(mongoSinkConfig.Collection);
                    var bulkWriteResult = await collection.BulkWriteAsync(models,
                        new BulkWriteOptions {IsOrdered = mongoSinkConfig.IsWriteOrdered});
                    _logger.Debug("Models written successfully to mongodb.",
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
            var mongoSourceConfig = _configurationProvider.GetSourceConfigProperties<MongoSourceConfig>(connector);
            var dbCollection = _mongoClientProvider.GetMongoClient(connector, taskId)
                .GetDatabase(mongoSourceConfig.Database)
                .GetCollection<BsonDocument>(collection);

            return await (await dbCollection.FindAsync(model.Model.Filter, model.Model.Options)).ToListAsync();
        }

        public async Task<IList<JsonNode>> ReadMany(IList<ConnectRecord<(FilterDefinition<BsonDocument>, FindOptions<BsonDocument>)>> batch, string connector, int taskId)
        {
            var mongoSourceConfig = _configurationProvider.GetSourceConfigProperties<MongoSourceConfig>(connector);
            var collection = _mongoClientProvider.GetMongoClient(connector, taskId)
                .GetDatabase(mongoSourceConfig.Database)
                .GetCollection<BsonDocument>("mongoSourceConfig.Collection");

            var data = await (await collection.FindAsync(batch[0].Model.Item1, batch[0].Model.Item2)).ToListAsync();
            return data.Select(d => JsonNode.Parse(d.ToJson())).ToList();
        }
    }
}