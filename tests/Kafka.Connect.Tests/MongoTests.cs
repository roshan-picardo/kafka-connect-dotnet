using System.Text.Json.Nodes;
using IntegrationTests.Kafka.Connect.Infrastructure;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;
using Xunit.Abstractions;

namespace IntegrationTests.Kafka.Connect;

[Collection("Integration Tests")]
public class MongoTests(TestFixture fixture, ITestOutputHelper output) : BaseTests<MongoSinkRecord>(fixture, output)
{
    private readonly TestFixture _fixture = fixture;
    private readonly ITestOutputHelper _output = output;

    [Theory, TestPriority(2)]
    [MemberData(nameof(TestCases), "MongoSink")]
    public async Task Execute(TestCase testCase) => await ExecuteTestAsync(testCase);

    protected override async Task SetupAsync(MongoSinkRecord sink)
    {
        if (sink.Setup.Length != 0)
        {
            var database = _fixture.GetMongoDatabase(sink.Database);
            var collection = database.GetCollection<BsonDocument>(sink.Collection);
        
            foreach (var item in sink.Setup)
            {
                var bsonDoc = BsonDocument.Parse(item.ToJsonString());
                await collection.InsertOneAsync(bsonDoc);
            }
        }
    }

    protected override async Task ValidateAsync(MongoSinkRecord sink)
    {
        if (sink.Expected.Length != 0)
        {
            var database = _fixture.GetMongoDatabase(sink.Database);
            var collection = database.GetCollection<BsonDocument>(sink.Collection);
        
            foreach (var expected in sink.Expected)
            {
                var expectedDoc = BsonDocument.Parse(expected.ToJsonString());
            
                if (expectedDoc.Contains(sink.KeyField))
                {
                    var filter = Builders<BsonDocument>.Filter.Eq(sink.KeyField, expectedDoc[sink.KeyField]);
                    var actualDoc = await collection.Find(filter).FirstOrDefaultAsync();
                
                    Assert.NotNull(actualDoc);
                
                    foreach (var element in expectedDoc.Elements.Where(e => e.Name != "_id"))
                    {
                        Assert.True(actualDoc.Contains(element.Name),
                            $"Field '{element.Name}' not found in actual document");
                        Assert.Equal(element.Value, actualDoc[element.Name]);
                    }
                }
            }
        }
    }

    protected override async Task CleanupAsync(MongoSinkRecord sink)
    {
        if (sink?.Cleanup?.Any() == true)
        {
            var database = _fixture.GetMongoDatabase(sink.Database);
            var collection = database.GetCollection<BsonDocument>(sink.Collection);
        
            foreach (var item in sink.Cleanup)
            {
                var cleanupDoc = BsonDocument.Parse(item.ToJsonString());
            
                if (cleanupDoc.Contains(sink.KeyField))
                {
                    var filter = Builders<BsonDocument>.Filter.Eq(sink.KeyField, cleanupDoc[sink.KeyField]);
                    await collection.DeleteOneAsync(filter);
                }
            }
        }
    }
}

public record MongoSinkRecord(string Database, string Collection, string KeyField, JsonNode[] Setup, JsonNode[] Expected, JsonNode[] Cleanup) : BaseSinkRecord;



