using System.Text.Json.Nodes;
using IntegrationTests.Kafka.Connect.Infrastructure;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Driver;
using Newtonsoft.Json;
using Xunit;
using Xunit.Abstractions;

namespace IntegrationTests.Kafka.Connect;

[Collection("Integration Tests")]
public class MongoTestRunner(TestFixture fixture, ITestOutputHelper output) : BaseTestRunner(fixture, output)
{
    private readonly TestFixture _fixture = fixture;
    private MongoClient? _mongoClient;
    private const string Target = "Mongo";

    private IMongoDatabase GetMongoDatabase(string databaseName)
    {
        _mongoClient ??= new MongoClient(_fixture.Configuration.GetServiceEndpoint("Mongo"));
        return _mongoClient.GetDatabase(databaseName);
    }

    [Theory, TestPriority(2)]
    [MemberData(nameof(TestCases), Target)]
    public async Task Execute(TestCase testCase) => await Run(testCase, Target);

    protected override async Task Insert(Dictionary<string, string> properties, TestCaseRecord record)
    {
        var database = GetMongoDatabase(properties["database"]);
        var collection = database.GetCollection<BsonDocument>(properties["collection"]);
        await collection.InsertOneAsync(BsonDocument.Parse(record.Value?.ToJsonString()));
    }

    protected override async Task Update(Dictionary<string, string> properties, TestCaseRecord record)
    {
        var database = GetMongoDatabase(properties["database"]);
        var collection = database.GetCollection<BsonDocument>(properties["collection"]);
        await collection.UpdateOneAsync(BsonDocument.Parse(record.Key?.ToJsonString()),
            new BsonDocument("$set", BsonDocument.Parse(record.Value?.ToJsonString())));
    }

    protected override async Task Delete(Dictionary<string, string> properties, TestCaseRecord record)
    {
        var database = GetMongoDatabase(properties["database"]);
        var collection = database.GetCollection<BsonDocument>(properties["collection"]);
        await collection.DeleteOneAsync(BsonDocument.Parse(record.Key?.ToJsonString()));
    }

    protected override async Task Setup(Dictionary<string, string> properties)
    {
        if (properties.TryGetValue("setup", out var script) && !string.IsNullOrEmpty(script))
        {
            var database = GetMongoDatabase(properties["database"]);
            // Execute MongoDB shell commands directly
            await ExecuteMongoScript(database, script);
        }
    }

    protected override async Task Cleanup(Dictionary<string, string> properties)
    {
        if (properties.TryGetValue("cleanup", out var script) && !string.IsNullOrEmpty(script))
        {
            var database = GetMongoDatabase(properties["database"]);
            // Execute MongoDB shell commands directly
            await ExecuteMongoScript(database, script);
        }
    }

    private static async Task ExecuteMongoScript(IMongoDatabase database, string script)
    {
        // Parse and execute MongoDB commands
        // Common patterns: db.collection.drop(), db.collection.deleteMany({}), etc.
        
        if (script.Contains(".drop()"))
        {
            // Extract collection name from pattern: db.collectionName.drop()
            var match = System.Text.RegularExpressions.Regex.Match(script, @"db\.(\w+)\.drop\(\)");
            if (match.Success)
            {
                var collectionName = match.Groups[1].Value;
                await database.DropCollectionAsync(collectionName);
            }
        }
        else if (script.Contains(".deleteMany"))
        {
            // Extract collection name from pattern: db.collectionName.deleteMany({})
            var match = System.Text.RegularExpressions.Regex.Match(script, @"db\.(\w+)\.deleteMany");
            if (match.Success)
            {
                var collectionName = match.Groups[1].Value;
                var collection = database.GetCollection<BsonDocument>(collectionName);
                await collection.DeleteManyAsync(FilterDefinition<BsonDocument>.Empty);
            }
        }
        // Add more command patterns as needed
    }

    protected override async Task<JsonNode?> Search(Dictionary<string, string> properties, TestCaseRecord record)
    {
        var database = GetMongoDatabase(properties["database"]);
        var collection = database.GetCollection<BsonDocument>(properties["collection"]);
        var actual = await collection.Find(BsonDocument.Parse(record.Key?.ToJsonString())).FirstOrDefaultAsync();
        return actual == null ? null : JsonNode.Parse(
            actual.ToJson(new JsonWriterSettings { OutputMode = JsonOutputMode.RelaxedExtendedJson }));
    }
}