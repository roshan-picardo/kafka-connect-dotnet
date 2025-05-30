using MongoDB.Driver;

namespace Kafka.Connect.MongoDb.Models;

public class FindModel<T>
{
    public string Operation { get; set; }
    public FilterDefinition<T> Filter { get; set; }
    public FindOptions<T> Options { get; set; }
}