using MongoDB.Driver;

namespace Kafka.Connect.MongoDb.Collections
{
    public interface IMongoClientProvider
    {
        IMongoClient GetMongoClient(string connector);
    }
}