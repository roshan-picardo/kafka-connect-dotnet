using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Kafka.Connect.MongoDb
{
    public interface IWriteModelStrategy
    {
        Task<(SinkStatus, IEnumerable<WriteModel<BsonDocument>>)> CreateWriteModels(SinkRecord sinkRecord);
    }
}