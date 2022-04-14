using Kafka.Connect.Mongodb.Models;

namespace Kafka.Connect.Mongodb.Collections
{
    public interface IMongoSinkConfigProvider
    {
        MongoSinkConfig GetMongoSinkConfig(string connector);
    }
}