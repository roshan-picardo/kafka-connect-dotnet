using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Mongodb.Models;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Models;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using Serilog.Context;
using Serilog.Core.Enrichers;

namespace Kafka.Connect.Mongodb.Collections
{
    public class MongoWriter : IMongoWriter
    {
        private readonly IMongoClientProvider _mongoClientProvider;
        private readonly ILogger<MongoWriter> _logger;

        public MongoWriter(IMongoClientProvider mongoClientProvider, ILogger<MongoWriter> logger)
        {
            _mongoClientProvider = mongoClientProvider;
            _logger = logger;
        }

        public async Task WriteMany(IList<MongoSinkRecord> batch, MongoSinkConfig mongoSinkConfig)
        {
            if (!mongoSinkConfig.HasTopicOverrides())
            {
                await Write(batch.Select(s => s.SinkRecord), mongoSinkConfig.Properties.Collection.Name,
                    BuildWriteModels(batch), mongoSinkConfig);
            }
            else
            {
                foreach (var (collection, topics) in mongoSinkConfig.GetCollectionTopicMaps(batch.Select(t => t.Topic)
                    .Distinct()))
                {
                    var records = batch.Where(b => topics.Contains(b.Topic)).Select(r => r.SinkRecord);
                    var writeModels = batch.Where(b => topics.Contains(b.Topic))
                        .SelectMany(wm => wm.WriteModels);
                    await Write(records, collection, writeModels, mongoSinkConfig);
                }
            }
        }

        public async Task CreateCollection(MongoSinkConfig config)
        {
            if (config.Properties?.Definition?.Create ?? false)
            {
                var mongoDatabase = _mongoClientProvider.GetMongoClient(config.Connector)
                    .GetDatabase(config.Properties.Database);
                var collectionName = (await mongoDatabase.ListCollectionNamesAsync(new ListCollectionNamesOptions
                    {Filter = new BsonDocument {{"name", config.Properties.Definition.Collection}}})).FirstOrDefault();

                if (string.IsNullOrEmpty(collectionName))
                {
                    _logger.LogInformation(
                        $"Attempting to create collection: {config.Properties.Definition.Collection}");
                    await mongoDatabase.CreateCollectionAsync(config.Properties.Definition.Collection);
                }

                var collection = mongoDatabase.GetCollection<BsonDocument>(config.Properties.Definition.Collection);

                var indexes = new List<CreateIndexModel<BsonDocument>>();

                foreach (var collectionIndex in config.Properties.Definition.Indexes)
                {
                    var bson = new BsonDocument();
                    foreach (var collectionIndexField in collectionIndex.Fields)
                    {
                        bson.Add(new BsonElement(collectionIndexField.Name, collectionIndexField.Order));
                    }

                    indexes.Add(new CreateIndexModel<BsonDocument>(bson,
                        new CreateIndexOptions {Unique = collectionIndex.Unique}));

                }

                await collection.Indexes.CreateManyAsync(indexes);
                _logger.LogInformation(
                    $"MongoDB collection '{config.Properties.Definition.Collection}' is setup and ready to use.");
            }
        }

        private async Task Write(IEnumerable<SinkRecord> records, string collectionName, IEnumerable<WriteModel<BsonDocument>> writeModels, MongoSinkConfig mongoSinkConfig)
        {
            var models = writeModels?.ToList();
            if (models == null || !models.Any())
            {
                return;
            }
            using (LogContext.Push(new PropertyEnricher("database", mongoSinkConfig.Properties.Database),
                new PropertyEnricher("collection", mongoSinkConfig.Properties.Collection)))
            {
                try
                {
                    var collection = _mongoClientProvider.GetMongoClient(mongoSinkConfig.Connector)
                        .GetDatabase(mongoSinkConfig.Properties.Database)
                        .GetCollection<BsonDocument>(collectionName);
                    var bulkWriteResult = await collection.BulkWriteAsync(models,
                        new BulkWriteOptions {IsOrdered = mongoSinkConfig.Properties.WriteStrategy.IsWriteOrdered});
                    _logger.LogDebug("{@debug}",
                        new
                        {
                            message = "Models written successfully to mongodb.",
                            result = new
                            {
                                acknowledged = bulkWriteResult.IsAcknowledged,
                                requests = bulkWriteResult.RequestCount,
                                deleted = bulkWriteResult.DeletedCount,
                                inserted = bulkWriteResult.InsertedCount,
                                matched = bulkWriteResult.MatchedCount,
                                modified = bulkWriteResult.ModifiedCount,
                                processed = bulkWriteResult.ProcessedRequests.Count,
                                upserts = bulkWriteResult.Upserts.Count,
                            }
                        });
                }
                catch (MongoBulkWriteException ex)
                {
                    throw new ConnectRetriableException(ErrorCode.Local_Application, ex).SetLogContext(records);
                }
                catch (MongoException me)
                {
                    throw new ConnectRetriableException(ErrorCode.Local_Application, me).SetLogContext(records);
                }
            }
        }

        private IList<WriteModel<BsonDocument>> BuildWriteModels(IEnumerable<MongoSinkRecord> batch)
        {
            var writeModels = new List<WriteModel<BsonDocument>>();
            foreach (var mongoSinkRecord in batch)
            {
                writeModels.AddRange(mongoSinkRecord.WriteModels);
            }

            _logger.LogTrace("{@debug}",
                new
                {
                    models = writeModels.Count,
                    message = "Preparing to write models to mongodb."
                });
            return writeModels;
        }
    }
}