using MongoDB.Driver;
using MongoDB.Bson;
using Confluent.Kafka;
using Newtonsoft.Json;
using Xunit;
using Xunit.Abstractions;
using Microsoft.Extensions.Logging;
using IntegrationTests.Kafka.Connect.Infrastructure;

namespace IntegrationTests.Kafka.Connect.Plugins;

public class MongoDbSinkTests : IClassFixture<DockerTestEnvironment>
{
    private readonly DockerTestEnvironment _testEnvironment;
    private readonly ITestOutputHelper _output;
    private readonly ILogger<MongoDbSinkTests> _logger;

    public MongoDbSinkTests(DockerTestEnvironment testEnvironment, ITestOutputHelper output)
    {
        _testEnvironment = testEnvironment;
        _output = output;
        
        using var loggerFactory = LoggerFactory.Create(builder => 
            builder.AddConsole().AddProvider(new XunitLoggerProvider(output)));
        _logger = loggerFactory.CreateLogger<MongoDbSinkTests>();
    }

    [Fact]
    public async Task SendMessage_ToKafkaTopic_ShouldSinkToMongoDB()
    {
        // Arrange
        const string topicName = "mongo-sink-test";
        const string collectionName = "mongo_sink_test";
        
        var testMessage = new
        {
            Id = Guid.NewGuid().ToString(),
            Name = "Test User",
            Email = "test@example.com",
            Age = 30,
            Timestamp = DateTime.UtcNow,
            Metadata = new
            {
                Source = "integration-test",
                Version = "1.0"
            }
        };

        var messageJson = JsonConvert.SerializeObject(testMessage);
        _logger.LogInformation("Sending test message: {Message}", messageJson);

        // Act - Send message to Kafka topic
        var deliveryResult = await _testEnvironment.KafkaProducer.ProduceAsync(
            topicName,
            new Message<string, string>
            {
                Key = testMessage.Id,
                Value = messageJson
            });

        _logger.LogInformation("Message sent to Kafka. Offset: {Offset}, Partition: {Partition}", 
            deliveryResult.Offset, deliveryResult.Partition);

        // Wait for kafka-connect to process the message
        await Task.Delay(TimeSpan.FromSeconds(10));

        // Assert - Verify message was sinked to MongoDB
        var collection = _testEnvironment.MongoDatabase.GetCollection<BsonDocument>(collectionName);
        
        var filter = Builders<BsonDocument>.Filter.Eq("Id", testMessage.Id);
        var document = await collection.Find(filter).FirstOrDefaultAsync();

        Assert.NotNull(document);
        Assert.Equal(testMessage.Name, document["Name"].AsString);
        Assert.Equal(testMessage.Email, document["Email"].AsString);
        Assert.Equal(testMessage.Age, document["Age"].AsInt32);
        
        _logger.LogInformation("Successfully verified message in MongoDB: {DocumentId}", document["_id"]);
    }

    [Fact]
    public async Task SendMultipleMessages_ToKafkaTopic_ShouldSinkAllToMongoDB()
    {
        // Arrange
        const string topicName = "mongo-sink-test";
        const string collectionName = "mongo_sink_test";
        const int messageCount = 5;

        var testMessages = Enumerable.Range(1, messageCount)
            .Select(i => new
            {
                Id = Guid.NewGuid().ToString(),
                Name = $"Test User {i}",
                Email = $"test{i}@example.com",
                Age = 20 + i,
                Timestamp = DateTime.UtcNow.AddMinutes(-i),
                BatchNumber = i
            })
            .ToList();

        // Act - Send multiple messages to Kafka topic
        var tasks = testMessages.Select(async message =>
        {
            var messageJson = JsonConvert.SerializeObject(message);
            return await _testEnvironment.KafkaProducer.ProduceAsync(
                topicName,
                new Message<string, string>
                {
                    Key = message.Id,
                    Value = messageJson
                });
        });

        var deliveryResults = await Task.WhenAll(tasks);
        _logger.LogInformation("Sent {Count} messages to Kafka", messageCount);

        // Wait for kafka-connect to process all messages
        await Task.Delay(TimeSpan.FromSeconds(15));

        // Assert - Verify all messages were sinked to MongoDB
        var collection = _testEnvironment.MongoDatabase.GetCollection<BsonDocument>(collectionName);
        
        foreach (var testMessage in testMessages)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("Id", testMessage.Id);
            var document = await collection.Find(filter).FirstOrDefaultAsync();

            Assert.NotNull(document);
            Assert.Equal(testMessage.Name, document["Name"].AsString);
            Assert.Equal(testMessage.Email, document["Email"].AsString);
            Assert.Equal(testMessage.Age, document["Age"].AsInt32);
            Assert.Equal(testMessage.BatchNumber, document["BatchNumber"].AsInt32);
        }

        _logger.LogInformation("Successfully verified all {Count} messages in MongoDB", messageCount);
    }

    [Fact]
    public async Task QueryMongoDatabase_AfterSinking_ShouldReturnCorrectData()
    {
        // Arrange
        const string topicName = "mongo-sink-test";
        const string collectionName = "mongo_sink_test";
        
        var userEvent = new
        {
            Id = Guid.NewGuid().ToString(),
            EventType = "user_registration",
            UserId = "user_123",
            UserName = "john_doe",
            Email = "john.doe@example.com",
            Timestamp = DateTime.UtcNow,
            Properties = new
            {
                Source = "web_app",
                Campaign = "summer_2024",
                ReferralCode = "REF123"
            }
        };

        // Act - Send user event to Kafka
        var messageJson = JsonConvert.SerializeObject(userEvent);
        await _testEnvironment.KafkaProducer.ProduceAsync(
            topicName,
            new Message<string, string>
            {
                Key = userEvent.Id,
                Value = messageJson
            });

        _logger.LogInformation("Sent user event: {Event}", messageJson);

        // Wait for processing
        await Task.Delay(TimeSpan.FromSeconds(10));

        // Assert - Query MongoDB and verify data structure
        var collection = _testEnvironment.MongoDatabase.GetCollection<BsonDocument>(collectionName);
        
        // Query by event type
        var eventTypeFilter = Builders<BsonDocument>.Filter.Eq("EventType", "user_registration");
        var eventDocuments = await collection.Find(eventTypeFilter).ToListAsync();
        
        Assert.NotEmpty(eventDocuments);
        
        var userDocument = eventDocuments.FirstOrDefault(d => d["Id"].AsString == userEvent.Id);
        Assert.NotNull(userDocument);
        
        // Verify nested properties
        Assert.Equal(userEvent.UserId, userDocument["UserId"].AsString);
        Assert.Equal(userEvent.UserName, userDocument["UserName"].AsString);
        Assert.Equal(userEvent.Email, userDocument["Email"].AsString);
        
        // Verify nested object
        var properties = userDocument["Properties"].AsBsonDocument;
        Assert.Equal("web_app", properties["Source"].AsString);
        Assert.Equal("summer_2024", properties["Campaign"].AsString);
        Assert.Equal("REF123", properties["ReferralCode"].AsString);

        _logger.LogInformation("Successfully queried and verified MongoDB data structure");

        // Additional query tests
        var countByEventType = await collection.CountDocumentsAsync(eventTypeFilter);
        Assert.True(countByEventType >= 1);

        var recentEvents = await collection
            .Find(Builders<BsonDocument>.Filter.Gte("Timestamp", DateTime.UtcNow.AddHours(-1)))
            .SortByDescending(d => d["Timestamp"])
            .Limit(10)
            .ToListAsync();
        
        Assert.NotEmpty(recentEvents);
        _logger.LogInformation("Found {Count} recent events in MongoDB", recentEvents.Count);
    }
}

// Helper class for XUnit logging
public class XunitLoggerProvider : ILoggerProvider
{
    private readonly ITestOutputHelper _output;

    public XunitLoggerProvider(ITestOutputHelper output)
    {
        _output = output;
    }

    public ILogger CreateLogger(string categoryName)
    {
        return new XunitLogger(_output, categoryName);
    }

    public void Dispose() { }
}

public class XunitLogger : ILogger
{
    private readonly ITestOutputHelper _output;
    private readonly string _categoryName;

    public XunitLogger(ITestOutputHelper output, string categoryName)
    {
        _output = output;
        _categoryName = categoryName;
    }

    public IDisposable BeginScope<TState>(TState state) => null!;
    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        _output.WriteLine($"[{logLevel}] {_categoryName}: {formatter(state, exception)}");
    }
}