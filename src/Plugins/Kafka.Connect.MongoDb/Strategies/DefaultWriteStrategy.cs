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
                var document = BsonDocument.Parse(record.Deserialized.Value.ToString());
                var keyDoc = record.Deserialized.Key == null || record.Deserialized.Key.Type == JTokenType.Null ||
                             record.Deserialized.Key.Type == JTokenType.None
                    ? new JObject { { "id", Guid.NewGuid() } }
                    : new JObject { { "id", record.Deserialized.Key } };
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