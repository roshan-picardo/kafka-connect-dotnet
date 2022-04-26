using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;
using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json.Linq;

namespace Kafka.Connect.Mongodb.Strategies
{
    public class DefaultWriteModelStrategy : IWriteModelStrategy
    {
        public async Task<(SinkStatus, IEnumerable<WriteModel<BsonDocument>>)> CreateWriteModels(SinkRecord sinkRecord)
        {
            var document = BsonDocument.Parse(sinkRecord.Value.ToString());
            var keyDoc = sinkRecord.Key == null || sinkRecord.Key.Type == JTokenType.Null ||
                         sinkRecord.Key.Type == JTokenType.None
                ? new JObject {{"id", Guid.NewGuid()}}
                : new JObject {{"id", sinkRecord.Key}};
            var key = BsonDocument.Parse(keyDoc.ToString());
            //convert JToken to BSON
            var model = new UpdateOneModel<BsonDocument>(
                new BsonDocumentFilterDefinition<BsonDocument>(new BsonDocument(key)),
                new BsonDocumentUpdateDefinition<BsonDocument>(new BsonDocument("$set", document))) { IsUpsert = true};

            return await Task.FromResult((SinkStatus.Updating, new List<UpdateOneModel<BsonDocument>>() {model}));
        }
    }
}