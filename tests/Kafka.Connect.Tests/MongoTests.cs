using IntegrationTests.Kafka.Connect.Infrastructure;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;
using Xunit.Abstractions;

namespace IntegrationTests.Kafka.Connect;

[Collection("Integration Tests")]
public class MongoTests(TestFixture fixture, ITestOutputHelper output) : BaseTests<MongoProperties>(fixture, output)
{
    private readonly TestFixture _fixture = fixture;

    [Theory, TestPriority(2)]
    [MemberData(nameof(TestCases), "Mongo")]
    public async Task Execute(TestCase<MongoProperties> testCase) => await ExecuteTest(testCase);

    protected override async Task Insert(MongoProperties properties, TestCaseRecord record)
    {
        var database = _fixture.GetMongoDatabase(properties.Database);
        var collection = database.GetCollection<BsonDocument>(properties.Collection);
        await collection.InsertOneAsync(BsonDocument.Parse(record.Value?.ToJsonString()));
    }

    protected override async Task Update(MongoProperties properties, TestCaseRecord record)
    {
        var database = _fixture.GetMongoDatabase(properties.Database);
        var collection = database.GetCollection<BsonDocument>(properties.Collection);
        await collection.UpdateOneAsync(BsonDocument.Parse(record.Key?.ToJsonString()), BsonDocument.Parse(record.Value?.ToJsonString()));
    }

    protected override async Task Delete(MongoProperties properties, TestCaseRecord record)
    {
        var database = _fixture.GetMongoDatabase(properties.Database);
        var collection = database.GetCollection<BsonDocument>(properties.Collection);
        await collection.DeleteOneAsync(BsonDocument.Parse(record.Key?.ToJsonString()));
    }

    protected override Task Setup(MongoProperties properties)
    {
        return Task.CompletedTask;
    }

    protected override Task Cleanup(MongoProperties properties)
    {
        return Task.CompletedTask;
    }

    protected override async Task Search(MongoProperties properties, TestCaseRecord record)
    {
        var database = _fixture.GetMongoDatabase(properties.Database);
        var collection = database.GetCollection<BsonDocument>(properties.Collection);
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

public record MongoProperties(string Database, string Collection) : TargetProperties
{
    public override string ToString()
    {
        return $"Database: {Database}, Collection: {Collection}";
    }
};






