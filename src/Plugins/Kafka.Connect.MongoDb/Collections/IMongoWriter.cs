using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.MongoDb.Models;

namespace Kafka.Connect.MongoDb.Collections
{
    public interface IMongoWriter
    {
        Task WriteMany(IList<MongoSinkRecord> batch, MongoSinkConfig mongoSinkConfig, string connector);
    }
}