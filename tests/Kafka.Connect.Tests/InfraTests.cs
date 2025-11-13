using MongoDB.Driver;
using MongoDB.Bson;
using Xunit;
using Xunit.Abstractions;
using IntegrationTests.Kafka.Connect.Infrastructure;

namespace IntegrationTests.Kafka.Connect;

[Collection("Integration Tests")]
public class InfraTests(Infrastructure.TestFixture fixture, ITestOutputHelper output)
{
    [Fact, TestPriority(1)]
    public async Task Kafka()
    {
        const string topicName = "infra-test-mongo";
        var testData = new
        {
            Id = Guid.NewGuid(),
            Timestamp = DateTime.UtcNow,
            Message = "Test connectivity message"
        };
        var testKey = testData.Id.ToString();
        var testValue = System.Text.Json.JsonSerializer.Serialize(testData);

        try
        {
            output.WriteLine($"Attempting to create Kafka topic: {topicName}");
            await fixture.CreateTopicAsync(topicName);
            output.WriteLine($"Kafka access confirmed - Created topic: {topicName}");

            var deliveryResult = await fixture.ProduceMessageAsync(topicName, testKey, testValue);
            output.WriteLine($"Produced message - Offset: {deliveryResult.Offset}, Partition: {deliveryResult.Partition}");

            var consumedMessage = await fixture.ConsumeMessageAsync(topicName, TimeSpan.FromSeconds(10));
            output.WriteLine($"Consumed message - Key: {consumedMessage.Message.Key}");

            Assert.Equal(topicName, deliveryResult.Topic);
            Assert.Equal(testKey, deliveryResult.Key);
            Assert.Equal(testValue, deliveryResult.Value);
            Assert.True(deliveryResult.Offset >= 0);
                
            Assert.Equal(testKey, consumedMessage.Message.Key);
            Assert.Equal(testValue, consumedMessage.Message.Value);
        }
        finally
        {
            await fixture.DeleteTopicAsync(topicName);
            output.WriteLine($"Cleaned up topic: {topicName}");
        }
    }

    [Fact, TestPriority(1)]
    public async Task MongoDb()
    {
        const string collectionName = "test-events";
        var database = fixture.GetMongoDatabase("infra-test-mongo");
        var testData = new BsonDocument
        {
            ["_id"] = ObjectId.GenerateNewId(),
            ["guid"] = Guid.NewGuid().ToString(),
            ["timestamp"] = DateTime.UtcNow,
            ["message"] = "Test connectivity message"
        };

        try
        {
            await database.CreateCollectionAsync(collectionName);
            var collection = database.GetCollection<BsonDocument>(collectionName);
            output.WriteLine($"MongoDB access confirmed - Created collection: {collectionName}");

            await collection.InsertOneAsync(testData);
            output.WriteLine($"Created document with ID: {testData["_id"]}");

            var filter = Builders<BsonDocument>.Filter.Eq("_id", testData["_id"]);
            var foundDocument = await collection.Find(filter).FirstOrDefaultAsync();
            output.WriteLine($"Read document: {foundDocument != null}");

            var update = Builders<BsonDocument>.Update.Set("message", "Updated test message");
            var updateResult = await collection.UpdateOneAsync(filter, update);
            output.WriteLine($"Updated document: {updateResult.ModifiedCount} modified");

            var deleteResult = await collection.DeleteOneAsync(filter);
            output.WriteLine($"Deleted document: {deleteResult.DeletedCount} deleted");

            Assert.NotNull(foundDocument);
            Assert.Equal(testData["guid"], foundDocument["guid"]);
            Assert.Equal(1, updateResult.ModifiedCount);
            Assert.Equal(1, deleteResult.DeletedCount);
        }
        finally
        {
            await database.DropCollectionAsync(collectionName);
            output.WriteLine($"Cleaned up collection: {collectionName}");
        }
    }
}