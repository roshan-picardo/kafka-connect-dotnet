using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.MongoDb.Models;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Kafka.Connect.MongoDb.Collections
{
    public interface IMongoQueryRunner
    {
        Task WriteMany(IList<WriteModel<BsonDocument>> models, string connector, int taskId);

        Task<IList<BsonDocument>> ReadMany(
            StrategyModel<FindModel<BsonDocument>> model,
            string connector,
            int taskId,
            string collection);
    }
}