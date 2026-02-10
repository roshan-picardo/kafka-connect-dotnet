using Amazon.DynamoDBv2.Model;

namespace Kafka.Connect.DynamoDb.Models;

public class QueryModel
{
    public string Operation { get; set; }
    public QueryRequest Request { get; set; }
}
