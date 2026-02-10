using Amazon.DynamoDBv2.Model;

namespace Kafka.Connect.DynamoDb.Models;

public class ScanModel
{
    public string Operation { get; set; }
    public ScanRequest Request { get; set; }
}
