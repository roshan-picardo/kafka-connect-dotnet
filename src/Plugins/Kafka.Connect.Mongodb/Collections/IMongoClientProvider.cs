using MongoDB.Driver;

namespace Kafka.Connect.Mongodb.Collections
{
    public interface IMongoClientProvider
    {
        IMongoClient GetMongoClient(string connector);
    }
}