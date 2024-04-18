using MongoDB.Driver;

namespace Kafka.Connect.MongoDb.Models;

public class FindModel<T>
{
    public FilterDefinition<T> Filter { get; set; }
    public FindOptions<T> Options { get; set; }
}