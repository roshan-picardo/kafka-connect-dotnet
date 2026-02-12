using System.Collections.Generic;
using System.Linq;
using Amazon.DynamoDBv2;

namespace Kafka.Connect.DynamoDb.Collections;

public class DynamoDbClientProvider : IDynamoDbClientProvider
{
    private readonly IEnumerable<IAmazonDynamoDB> _dynamoDbClients;
    private readonly IEnumerable<AmazonDynamoDBStreamsClient> _streamsClients;

    public DynamoDbClientProvider(
        IEnumerable<IAmazonDynamoDB> dynamoDbClients,
        IEnumerable<AmazonDynamoDBStreamsClient> streamsClients)
    {
        _dynamoDbClients = dynamoDbClients;
        _streamsClients = streamsClients;
    }
    
    public IAmazonDynamoDB GetDynamoDbClient(string connector, int taskId)
    {
        return _dynamoDbClients.ElementAtOrDefault(taskId - 1);
    }
    
    public AmazonDynamoDBStreamsClient GetStreamsClient(string connector, int taskId)
    {
        return _streamsClients.ElementAtOrDefault(taskId - 1);
    }
}
