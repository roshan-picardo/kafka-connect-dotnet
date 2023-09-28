using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.MongoDb.Models;
using Kafka.Connect.Plugin.Models;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Kafka.Connect.MongoDb.Collections
{
    public interface IMongoWriter
    {
        Task WriteMany(IList<MongoSinkRecord> batch, MongoSinkConfig mongoSinkConfig, string connector);
        Task WriteMany(IList<SinkRecord<WriteModel<BsonDocument>>> batch, string connector);
    }
}