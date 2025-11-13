using System.Collections;
using System.Text.Json;
using System.Text.Json.Nodes;
using IntegrationTests.Kafka.Connect.Infrastructure;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;
using Xunit.Abstractions;

namespace IntegrationTests.Kafka.Connect;

[Collection("Integration Tests")]
public class MongoTests(TestFixture fixture, ITestOutputHelper output) : BaseIntegrationTest<MongoTestCase>(fixture, output)
{
    private readonly TestFixture _fixture = fixture;
    private readonly ITestOutputHelper _output = output;

    [Theory, TestPriority(2)]
    [ClassData(typeof(TestCaseBuilder))]
    public async Task ExecuteMongoSinkTest(MongoTestCase testCase)
    {
        await ExecuteTestAsync(testCase);
    }

    protected override async Task SetupAsync(MongoTestCase testCase)
    {
        if (testCase.Sink.Setup?.Any() == true)
        {
            await SetupMongoData(testCase.Sink.Database, testCase.Sink.Collection, testCase.Sink.Setup);
        }
    }

    protected override async Task ValidateAsync(MongoTestCase testCase)
    {
        if (testCase.Sink.Expected?.Any() == true)
        {
            await ValidateMongoData(testCase.Sink.Database, testCase.Sink.Collection, testCase.Sink.KeyField, testCase.Sink.Expected);
        }
    }

    protected override async Task CleanupAsync(MongoTestCase testCase)
    {
        if (testCase.Sink.Cleanup?.Any() == true)
        {
            await CleanupMongoData(testCase.Sink.Database, testCase.Sink.Collection, testCase.Sink.KeyField, testCase.Sink.Cleanup);
        }
    }

    protected override string GetTestTitle(MongoTestCase testCase)
    {
        return testCase.Title;
    }

    protected override string GetTopicName(MongoTestCase testCase)
    {
        return testCase.Sink.Topic;
    }

    protected override IEnumerable<(string Key, string Value)> GetRecords(MongoTestCase testCase)
    {
        return testCase.Records.Select(record => (
            Key: record.Key?.ToString() ?? "",
            Value: record.Value.ToJsonString()
        ));
    }


    private async Task SetupMongoData(string databaseName, string collectionName, JsonNode[] setupData)
    {
        var database = _fixture.GetMongoDatabase(databaseName);
        var collection = database.GetCollection<BsonDocument>(collectionName);
        
        foreach (var item in setupData)
        {
            var bsonDoc = BsonDocument.Parse(item.ToJsonString());
            await collection.InsertOneAsync(bsonDoc);
        }
        
        _output.WriteLine($"Setup: Inserted {setupData.Length} documents into {databaseName}.{collectionName}");
    }



    private async Task ValidateMongoData(string databaseName, string collectionName, string keyField, JsonNode[] expectedData)
    {
        var database = _fixture.GetMongoDatabase(databaseName);
        var collection = database.GetCollection<BsonDocument>(collectionName);
        
        foreach (var expected in expectedData)
        {
            var expectedDoc = BsonDocument.Parse(expected.ToJsonString());
            
            if (expectedDoc.Contains(keyField))
            {
                var filter = Builders<BsonDocument>.Filter.Eq(keyField, expectedDoc[keyField]);
                var actualDoc = await collection.Find(filter).FirstOrDefaultAsync();
                
                Assert.NotNull(actualDoc);
                
                foreach (var element in expectedDoc.Elements.Where(e => e.Name != "_id"))
                {
                    Assert.True(actualDoc.Contains(element.Name),
                        $"Field '{element.Name}' not found in actual document");
                    Assert.Equal(element.Value, actualDoc[element.Name]);
                }
                
                _output.WriteLine($"Validation passed for {keyField}: {expectedDoc[keyField]}");
            }
        }
    }

    private async Task CleanupMongoData(string databaseName, string collectionName, string keyField, JsonNode[] cleanupData)
    {
        var database = _fixture.GetMongoDatabase(databaseName);
        var collection = database.GetCollection<BsonDocument>(collectionName);
        
        foreach (var item in cleanupData)
        {
            var cleanupDoc = BsonDocument.Parse(item.ToJsonString());
            
            if (cleanupDoc.Contains(keyField))
            {
                var filter = Builders<BsonDocument>.Filter.Eq(keyField, cleanupDoc[keyField]);
                await collection.DeleteOneAsync(filter);
            }
        }
        
        _output.WriteLine($"Cleanup: Removed {cleanupData.Length} documents from {databaseName}.{collectionName}");
    }

    public override void Dispose()
    {
        base.Dispose();
    }
}

// Data models for MongoDB tests
public record MongoTestConfig(
    string Topic,
    string? Schema,
    string? Folder,
    string[]? Files
);

public record MongoTestData(
    string? Title,
    string? Schema,
    MongoRecord[]? Records,
    MongoSink? Sink
);

public record MongoRecord(JsonNode? Key, JsonNode Value);
public record MongoSchemaRecord(JsonNode? Key, JsonNode Value);
public record MongoSink(
    string Type = "mongodb",
    string Topic = "test-topic",
    string Collection = "test_collection",
    string Database = "kafka_connect_test",
    string KeyField = "id",
    JsonNode[]? Setup = null,
    JsonNode[]? Expected = null,
    JsonNode[]? Cleanup = null
);

public record MongoTestCase(
    string Title,
    MongoSchemaRecord Schema,
    MongoRecord[] Records,
    MongoSink Sink)
{
    public override string ToString() => Title;
}
