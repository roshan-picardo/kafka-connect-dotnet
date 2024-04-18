using Kafka.Connect.Plugin.Models;
using MongoDB.Driver;

namespace Kafka.Connect.MongoDb.Models;

public class UpdateModel<T> 
{
    public SinkStatus Status { get; set; }
    public FilterDefinition<T> Filter { get; set; }
    public FindOptions<T> Options { get; set; }
}