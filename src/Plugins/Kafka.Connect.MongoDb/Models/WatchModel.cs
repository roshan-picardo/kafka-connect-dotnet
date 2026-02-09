using MongoDB.Bson;
using MongoDB.Driver;

namespace Kafka.Connect.MongoDb.Models;

public class WatchModel
{
    public string Operation { get; set; }
    public BsonDocument ResumeToken { get; set; }
    public ChangeStreamOptions Options { get; set; }
}
