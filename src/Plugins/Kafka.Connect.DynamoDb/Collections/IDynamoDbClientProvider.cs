using Amazon.DynamoDBv2;

namespace Kafka.Connect.DynamoDb.Collections;

public interface IDynamoDbClientProvider
{
    IAmazonDynamoDB GetDynamoDbClient(string connector, int taskId);
}
