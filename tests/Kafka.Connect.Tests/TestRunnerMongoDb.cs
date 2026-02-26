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
public class TestRunnerMongoDb(TestFixture fixture, ITestOutputHelper output) : BaseTestRunner(fixture, output)
{
    private readonly TestFixture _fixture = fixture;
    private MongoClient? _mongoClient;
    private const string Target = "MongoDB";

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

    protected override Task Setup(Dictionary<string, string> properties)
    {
        return Task.CompletedTask;
    }

    protected override Task Cleanup(Dictionary<string, string> properties)
    {
        return Task.CompletedTask;
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