using Kafka.Connect.MongoDb.Models;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace UnitTests.Kafka.Connect.MongoDb.Models;

public class FindAndWatchModelTests
{
    [Fact]
    public void FindModel_AssignsProperties()
    {
        var model = new FindModel<BsonDocument>
        {
            Operation = "CHANGE",
            Filter = Builders<BsonDocument>.Filter.Empty,
            Options = new FindOptions<BsonDocument> { BatchSize = 10 }
        };

        Assert.Equal("CHANGE", model.Operation);
        Assert.NotNull(model.Filter);
        Assert.Equal(10, model.Options.BatchSize);
    }

    [Fact]
    public void WatchModel_AssignsProperties()
    {
        var resume = BsonDocument.Parse("{\"_data\":\"abc\"}");
        var model = new WatchModel
        {
            Operation = "STREAM",
            ResumeToken = resume,
            Options = new ChangeStreamOptions { BatchSize = 5 }
        };

        Assert.Equal("STREAM", model.Operation);
        Assert.Equal(resume, model.ResumeToken);
        Assert.Equal(5, model.Options.BatchSize);
    }
}
