using IntegrationTests.Kafka.Connect.Infrastructure;
using MongoDB.Bson;
using MongoDB.Driver;
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
        _mongoClient ??= new MongoClient(_fixture.Configuration.Shakedown.Mongo);
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
        await collection.UpdateOneAsync(BsonDocument.Parse(record.Key?.ToJsonString()), BsonDocument.Parse(record.Value?.ToJsonString()));
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

    protected override async Task Search(Dictionary<string, string> properties, TestCaseRecord record)
    {
        var database = GetMongoDatabase(properties["database"]);
        var collection = database.GetCollection<BsonDocument>(properties["collection"]);
        var actual = await collection.Find(BsonDocument.Parse(record.Key?.ToJsonString())).FirstOrDefaultAsync();
        if (record.Value != null)
        {
            Assert.NotNull(actual);
            var expected = BsonDocument.Parse(record.Value.ToJsonString());
            foreach (var element in expected.Elements)
            {
                Assert.True(expected.Contains(element.Name),
                    $"Field '{element.Name}' not found in actual document");
                Assert.Equal(element.Value, actual[element.Name]);
            }
        }
        else
        {
            Assert.Null(actual);
        }
    }
}