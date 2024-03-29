using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Kafka.Connect.MongoDb.Collections
{
    public interface IMongoWriter
    {
        Task WriteMany(IList<ConnectRecord<WriteModel<BsonDocument>>> batch, string connector, int taskId);
    }
}