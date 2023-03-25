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

        public async Task WriteMany(IList<MongoSinkRecord> batch, MongoSinkConfig mongoSinkConfig, string connector)
        {
                await Write(batch.Select(s => s.SinkRecord), BuildWriteModels(batch), mongoSinkConfig, connector);
        }

        private async Task Write(IEnumerable<SinkRecord> records, IEnumerable<WriteModel<BsonDocument>> writeModels, MongoSinkConfig mongoSinkConfig, string connector)
        {
            var models = writeModels?.ToList();
            if (models == null || !models.Any())
            {
                return;
            }
            using (LogContext.Push(new PropertyEnricher("database", mongoSinkConfig.Database),
                new PropertyEnricher("collection", mongoSinkConfig.Collection)))
            {
                try
                {
                    var collection = _mongoClientProvider.GetMongoClient(connector)
                        .GetDatabase(mongoSinkConfig.Database)
                        .GetCollection<BsonDocument>(mongoSinkConfig.Collection);
                    var bulkWriteResult = await collection.BulkWriteAsync(models,
                        new BulkWriteOptions {IsOrdered = mongoSinkConfig.WriteStrategy.IsWriteOrdered});
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