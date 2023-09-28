using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Connect.MongoDb.Models;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using MongoDB.Bson;
using MongoDB.Driver;
using Serilog.Context;
using Serilog.Core.Enrichers;

namespace Kafka.Connect.MongoDb.Collections
{
    public class MongoWriter : IMongoWriter
    {
        private readonly IMongoClientProvider _mongoClientProvider;
        private readonly IConfigurationProvider _configurationProvider;
        private readonly ILogger<MongoWriter> _logger;

        public MongoWriter(
            ILogger<MongoWriter> logger,
            IMongoClientProvider mongoClientProvider,
            IConfigurationProvider configurationProvider)
        {
            _logger = logger;
            _mongoClientProvider = mongoClientProvider;
            _configurationProvider = configurationProvider;
        }

        public async Task WriteMany(IList<MongoSinkRecord> batch, MongoSinkConfig mongoSinkConfig, string connector)
        {
            using (_logger.Track("Writing models to database"))
            {
                await Write(batch.Select(s => s.GetRecord()), BuildWriteModels(batch), connector);
            }
        }

        public async Task WriteMany(IList<SinkRecord<WriteModel<BsonDocument>>> batch, string connector)
        {
            using (_logger.Track("Writing models to database"))
            {
                await Write(batch.Select(s => s.GetRecord()), BuildWriteModels(batch), connector);
            }
        }

        private async Task Write(IEnumerable<SinkRecord> records, IEnumerable<WriteModel<BsonDocument>> writeModels, string connector)
        {
            var mongoSinkConfig = _configurationProvider.GetSinkConfigProperties<MongoSinkConfig>(connector);
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
                    throw new ConnectRetriableException(ex.Message, ex).SetLogContext(records);
                }
                catch (MongoException me)
                {
                    throw new ConnectRetriableException(me.Message, me).SetLogContext(records);
                }
            }
        }

        private IEnumerable<WriteModel<BsonDocument>> BuildWriteModels(IEnumerable<SinkRecord<WriteModel<BsonDocument>>> batch)
        {
            var writeModels = new List<WriteModel<BsonDocument>>();
            batch.ForEach(record => writeModels.AddRange(record.Models));

            _logger.Trace("Preparing to write models to mongodb.", new { Models = writeModels.Count });
            return writeModels;
        }
    }
}