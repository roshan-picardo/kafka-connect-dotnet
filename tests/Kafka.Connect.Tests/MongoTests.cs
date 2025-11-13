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
public class MongoTests(TestFixture fixture, ITestOutputHelper output) : BaseIntegrationTest<MongoSinkRecord>(fixture, output)
{
    private readonly TestFixture _fixture = fixture;
    private readonly ITestOutputHelper _output = output;

    [Theory, TestPriority(2)]
    [ClassData(typeof(TestCaseBuilder))]
    public async Task ExecuteMongoSinkTest(TestCase testCase)
    {
        await ExecuteTestAsync(testCase);
    }

    protected override async Task SetupAsync(MongoSinkRecord? sink)
    {
        if (sink?.Setup?.Any() == true)
        {
            await SetupMongoData(sink.Database, sink.Collection, sink.Setup);
        }
    }

    protected override async Task ValidateAsync(MongoSinkRecord? sink)
    {
        if (sink?.Expected?.Any() == true)
        {
            await ValidateMongoData(sink.Database, sink.Collection, sink.KeyField, sink.Expected);
        }
    }

    protected override async Task CleanupAsync(MongoSinkRecord? sink)
    {
        if (sink?.Cleanup?.Any() == true)
        {
            await CleanupMongoData(sink.Database, sink.Collection, sink.KeyField, sink.Cleanup);
        }
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

public record SchemaRecord(JsonNode? Key, JsonNode Value);
public record TestCaseConfig(string Schema, string? Folder, string[]? Files);

public record TestCase(string Title, KafkaRecord[] Records, SinkRecord<MongoSinkRecord> Sink);

public record KafkaRecord(JsonNode? Key, JsonNode Value);

public record SinkRecord<T>(string Topic = "", T? Properties = default);

public record BaseSinkRecord;
public record MongoSinkRecord(string Database, string Collection, string KeyField, JsonNode[] Setup, JsonNode[] Expected, JsonNode[] Cleanup) : BaseSinkRecord;


