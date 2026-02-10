using Amazon.DynamoDBv2.Model;

namespace Kafka.Connect.DynamoDb.Models;

public class StreamModel
{
    public string Operation { get; set; }
    public string ShardIterator { get; set; }
    public GetRecordsRequest Request { get; set; }
}
