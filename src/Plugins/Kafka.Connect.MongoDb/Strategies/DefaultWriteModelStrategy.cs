using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;
using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.MongoDb.Strategies
{
    public class DefaultWriteModelStrategy : WriteStrategy<WriteModel<BsonDocument>>
    {
        private readonly ILogger<DefaultWriteModelStrategy> _logger;

        public DefaultWriteModelStrategy(ILogger<DefaultWriteModelStrategy> logger)
        {
            _logger = logger;
        }
        public async Task<(SinkStatus, IEnumerable<WriteModel<BsonDocument>>)> CreateWriteModels(SinkRecord sinkRecord)
        {
            using (_logger.Track("Creating write models"))
            {
                var document = BsonDocument.Parse(sinkRecord.Value.ToString());
                var keyDoc = sinkRecord.Key == null || sinkRecord.Key.Type == JTokenType.Null ||
                             sinkRecord.Key.Type == JTokenType.None
                    ? new JObject { { "id", Guid.NewGuid() } }
                    : new JObject { { "id", sinkRecord.Key } };
                var key = BsonDocument.Parse(keyDoc.ToString());
                //convert JToken to BSON
                var model = new UpdateOneModel<BsonDocument>(
                        new BsonDocumentFilterDefinition<BsonDocument>(new BsonDocument(key)),
                        new BsonDocumentUpdateDefinition<BsonDocument>(new BsonDocument("$set", document)))
                    { IsUpsert = true };

                return await Task.FromResult((SinkStatus.Updating, new List<UpdateOneModel<BsonDocument>>() { model }));
            }
        }

        protected override async Task<(SinkStatus Status, IList<WriteModel<BsonDocument>> Models)> BuildModels(string connector, SinkRecord record)
        {
            using (_logger.Track("Creating write models"))
            {
                var document = BsonDocument.Parse(record.Value.ToString());
                var keyDoc = record.Key == null || record.Key.Type == JTokenType.Null ||
                             record.Key.Type == JTokenType.None
                    ? new JObject { { "id", Guid.NewGuid() } }
                    : new JObject { { "id", record.Key } };
                var key = BsonDocument.Parse(keyDoc.ToString());
                //convert JToken to BSON
                var model = new UpdateOneModel<BsonDocument>(
                        new BsonDocumentFilterDefinition<BsonDocument>(new BsonDocument(key)),
                        new BsonDocumentUpdateDefinition<BsonDocument>(new BsonDocument("$set", document)))
                    { IsUpsert = true };

                return await Task.FromResult((SinkStatus.Updating, new List<WriteModel<BsonDocument>> { model }));
            }
        }
    }
}