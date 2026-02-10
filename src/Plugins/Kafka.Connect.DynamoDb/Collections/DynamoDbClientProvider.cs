using System.Collections.Generic;
using System.Linq;
using Amazon.DynamoDBv2;

namespace Kafka.Connect.DynamoDb.Collections;

public class DynamoDbClientProvider : IDynamoDbClientProvider
{
    private readonly IEnumerable<IAmazonDynamoDB> _dynamoDbClients;

    public DynamoDbClientProvider(IEnumerable<IAmazonDynamoDB> dynamoDbClients)
    {
        _dynamoDbClients = dynamoDbClients;
    }
    
    public IAmazonDynamoDB GetDynamoDbClient(string connector, int taskId)
    {
        return _dynamoDbClients.ElementAtOrDefault(taskId - 1);
    }
}
