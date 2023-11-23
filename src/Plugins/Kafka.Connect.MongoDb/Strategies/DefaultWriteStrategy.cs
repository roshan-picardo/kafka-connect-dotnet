using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Kafka.Connect.MongoDb.Strategies
{
    public class DefaultWriteStrategy : WriteStrategy<WriteModel<BsonDocument>>
    {
        private readonly ILogger<DefaultWriteStrategy> _logger;

        public DefaultWriteStrategy(ILogger<DefaultWriteStrategy> logger)
        {
            _logger = logger;
        }

        protected override async Task<(SinkStatus Status, IList<WriteModel<BsonDocument>> Models)> BuildModels(string connector, ConnectRecord record)
        {
            using (_logger.Track("Creating write models"))
            {
                var document = BsonDocument.Parse(record.Deserialized.Value.ToJsonString());
                var keyDoc = record.Deserialized.Key == null 
                    ? new JsonObject { { "id", Guid.NewGuid() } }
                    : new JsonObject { { "id", record.Deserialized.Key.ToString() } };
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