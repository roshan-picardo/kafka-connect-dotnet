using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Mongodb.Models;

namespace Kafka.Connect.Mongodb.Collections
{
    public interface IMongoWriter
    {
        Task WriteMany(IList<MongoSinkRecord> batch, MongoSinkConfig mongoSinkConfig, string connector);
    }
}