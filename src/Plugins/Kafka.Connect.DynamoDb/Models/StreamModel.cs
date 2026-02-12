using Amazon.DynamoDBv2.Model;

namespace Kafka.Connect.DynamoDb.Models;

public class StreamModel
{
    public string Operation { get; set; }
    public string TableName { get; set; }
    public string StreamArn { get; set; }
    public string ShardId { get; set; }
    public string ShardIterator { get; set; }
    public string ShardIteratorType { get; set; }
    public string SequenceNumber { get; set; }
    public GetRecordsRequest Request { get; set; }
}
