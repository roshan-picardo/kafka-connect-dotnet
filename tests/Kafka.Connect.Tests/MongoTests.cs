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
public class MongoTests(TestFixture fixture, ITestOutputHelper output) : IDisposable
{
    [Theory]
    [ClassData(typeof(MongoTestCaseBuilder))]
    public async Task ExecuteMongoSinkTest(MongoTestCase testCase)
    {
        output.WriteLine($"Executing test: {testCase.Title}");

        try
        {
            if (testCase.Sink.Setup?.Any() == true)
            {
                await SetupMongoData(testCase.Sink.Database, testCase.Sink.Collection, testCase.Sink.Setup);
            }

            await fixture.CreateTopicAsync(testCase.Sink.Topic);
            await SendMessagesToKafka(testCase.Sink.Topic, testCase.Records);

            await Task.Delay(5000);

            if (testCase.Sink.Expected?.Any() == true)
            {
                await ValidateMongoData(testCase.Sink.Database, testCase.Sink.Collection, testCase.Sink.KeyField, testCase.Sink.Expected);
            }

            output.WriteLine($"Test '{testCase.Title}' completed successfully");
        }
        finally
        {
            if (testCase.Sink.Cleanup?.Any() == true)
            {
                await CleanupMongoData(testCase.Sink.Database, testCase.Sink.Collection, testCase.Sink.KeyField, testCase.Sink.Cleanup);
            }
        }
    }


    private async Task SetupMongoData(string databaseName, string collectionName, JsonNode[] setupData)
    {
        var database = fixture.GetMongoDatabase(databaseName);
        var collection = database.GetCollection<BsonDocument>(collectionName);
        
        foreach (var item in setupData)
        {
            var bsonDoc = BsonDocument.Parse(item.ToJsonString());
            await collection.InsertOneAsync(bsonDoc);
        }
        
        output.WriteLine($"Setup: Inserted {setupData.Length} documents into {databaseName}.{collectionName}");
    }

    private async Task SendMessagesToKafka(string topicName, MongoRecord[] records)
    {
        foreach (var record in records)
        {
            var key = record.Key?.ToString() ?? "";
            var value = record.Value.ToJsonString();
            var result = await fixture.ProduceMessageAsync(topicName, key, value);
            output.WriteLine($"Sent message to {result.Topic}:{result.Partition}:{result.Offset}");
        }
    }


    private async Task ValidateMongoData(string databaseName, string collectionName, string keyField, JsonNode[] expectedData)
    {
        var database = fixture.GetMongoDatabase(databaseName);
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
                
                output.WriteLine($"Validation passed for {keyField}: {expectedDoc[keyField]}");
            }
        }
    }

    private async Task CleanupMongoData(string databaseName, string collectionName, string keyField, JsonNode[] cleanupData)
    {
        var database = fixture.GetMongoDatabase(databaseName);
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
        
        output.WriteLine($"Cleanup: Removed {cleanupData.Length} documents from {databaseName}.{collectionName}");
    }

    public void Dispose()
    {
    }
}

public class MongoTestCaseBuilder : IEnumerable<object[]>
{
    public IEnumerator<object[]> GetEnumerator()
    {
        var testDataBasePath = Path.Combine(Directory.GetCurrentDirectory(), "data");
        var configPath = Path.Combine(testDataBasePath, "mongo-test-config.json");
        
        if (!File.Exists(configPath))
        {
            yield break;
        }

        var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
        var configs = JsonSerializer.Deserialize<MongoTestConfig[]>(File.ReadAllText(configPath), options);
        
        if (configs == null) yield break;

        foreach (var config in configs)
        {
            MongoSchemaRecord? schema = null;
            if (!string.IsNullOrEmpty(config.Schema))
            {
                var schemaPath = Path.Combine(testDataBasePath, config.Schema.TrimStart('/'));
                if (File.Exists(schemaPath))
                {
                    var schemaNode = JsonNode.Parse(File.ReadAllText(schemaPath))?.AsObject();
                    var valueNode = schemaNode?["Value"];
                    if (valueNode != null)
                    {
                        schema = new MongoSchemaRecord(schemaNode?["Key"], valueNode);
                    }
                }
            }

            if (schema == null) continue;

            IList<string> testFiles = new List<string>();
            
            if (config.Files?.Any() == true)
            {
                foreach (var file in config.Files)
                {
                    var filePath = Path.Combine(testDataBasePath, file.TrimStart('/'));
                    if (File.Exists(filePath))
                    {
                        testFiles.Add(filePath);
                    }
                }
            }
            else if (!string.IsNullOrEmpty(config.Folder))
            {
                var folderPath = Path.Combine(testDataBasePath, config.Folder.TrimStart('/'));
                if (Directory.Exists(folderPath))
                {
                    testFiles = Directory.GetFiles(folderPath, "*.json")
                        .OrderBy(Path.GetFileName)
                        .ToList();
                }
            }

            foreach (var testFile in testFiles)
            {
                var testData = JsonSerializer.Deserialize<MongoTestData>(File.ReadAllText(testFile), options);
                if (testData == null) continue;

                yield return
                [
                    new MongoTestCase(
                        testData.Title ?? Path.GetFileNameWithoutExtension(testFile),
                        schema,
                        testData.Records ?? [],
                        testData.Sink ?? new MongoSink()
                    )
                ];
            }
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
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
