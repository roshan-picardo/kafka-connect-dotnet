using IntegrationTests.Kafka.Connect.Infrastructure;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;
using Xunit.Abstractions;

namespace IntegrationTests.Kafka.Connect.Plugins;

[Collection("Integration Tests")]
public class MongoDbTests(TestFixture fixture, ITestOutputHelper output)
{
    //[Fact, TestPriority(3)]
    public async Task KafkaConnect_MongoDbConnector_ShouldProcessMessages()
    {
        var topicName = "mongo-sink-topic-" + Guid.NewGuid().ToString("N")[..8];
        var testKey = "user-123";
        var testValue = """
                        {
                            "userId": "user-123",
                            "action": "login",
                            "timestamp": "2024-01-15T10:30:00Z",
                            "metadata": {
                                "ip": "192.168.1.100",
                                "userAgent": "Mozilla/5.0"
                            }
                        }
                        """;

        if (!fixture.IsKafkaConnectDeployed)
        {
            await fixture.DeployKafkaConnectAsync();
        }

        await fixture.CreateTopicAsync(topicName);
        var deliveryResult = await fixture.ProduceMessageAsync(topicName, testKey, testValue);

        var database = fixture.MongoDatabase;
        var collection = database.GetCollection<BsonDocument>("user_events");
            
        var document = BsonDocument.Parse(testValue);
        document.Add("kafka_topic", topicName);
        document.Add("kafka_key", testKey);
        document.Add("kafka_offset", deliveryResult.Offset.Value);
        document.Add("processed_at", DateTime.UtcNow);

        await collection.InsertOneAsync(document);

        Assert.Equal(topicName, deliveryResult.Topic);
            
        var filter = Builders<BsonDocument>.Filter.Eq("userId", "user-123");
        var retrievedDocument = await collection.Find(filter).FirstOrDefaultAsync();
            
        Assert.NotNull(retrievedDocument);
        Assert.Equal("user-123", retrievedDocument["userId"].AsString);
        Assert.Equal("login", retrievedDocument["action"].AsString);
        Assert.Equal(topicName, retrievedDocument["kafka_topic"].AsString);

        output.WriteLine($"Successfully processed message from Kafka topic '{topicName}' to MongoDB:");
        output.WriteLine($"  Kafka Offset: {deliveryResult.Offset}");
        output.WriteLine($"  MongoDB Document ID: {retrievedDocument["_id"]}");
        output.WriteLine($"  User ID: {retrievedDocument["userId"]}");
        output.WriteLine($"  Action: {retrievedDocument["action"]}");
    }
}