using MongoDB.Bson;
using MongoDB.Driver;

namespace Kafka.Connect.MongoDb.Models;

public class ChangeStreamModel<T>
{
    public string Operation { get; set; }
    public ChangeStreamOptions Options { get; set; }
    public PipelineDefinition<ChangeStreamDocument<T>, ChangeStreamDocument<T>> Pipeline { get; set; }
    public BsonTimestamp ResumeToken { get; set; }
    public int MaxAwaitTimeMs { get; set; } = 1000;
    public bool UseChangeStreams { get; set; }
}
