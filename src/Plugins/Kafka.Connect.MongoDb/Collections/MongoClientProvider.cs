using System.Collections.Generic;
using System.Linq;
using MongoDB.Driver;

namespace Kafka.Connect.MongoDb.Collections
{
    public class MongoClientProvider : IMongoClientProvider
    {
        private readonly IEnumerable<IMongoClient> _mongoClients;

        public MongoClientProvider(IEnumerable<IMongoClient> mongoClients)
        {
            _mongoClients = mongoClients;
        }
        public IMongoClient GetMongoClient(string connector, int taskId)
        {
            return _mongoClients.SingleOrDefault(m => m.Settings.ApplicationName == $"{connector}-{taskId:00}");
        }
    }
}