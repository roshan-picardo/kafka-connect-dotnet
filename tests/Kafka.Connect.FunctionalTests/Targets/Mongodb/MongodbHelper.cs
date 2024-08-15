using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Kafka.Connect.FunctionalTests.Targets.Mongodb;

public class MongodbHelper : ITargetHelper
{
    private readonly IMongoDatabase _mongoDatabase;

    public MongodbHelper(MongodbConfig config)
    {
        if(config.Disabled) return;
        var mongoClient = new MongoClient(config.ConnectionString);
        _mongoDatabase = mongoClient.GetDatabase(config.Database);
    }
    

    public Task Setup(Sink data)
    {
        throw new NotImplementedException();
        // if (data.Type == TargetType.Mongodb && data.Setup.HasValues)
        // {
        //     var mongoCollection = _mongoDatabase.GetCollection<BsonDocument>(data.Destination);
        //
        //     switch (data.Setup)
        //     {
        //         case JArray array:
        //         {
        //             foreach (var document in array)
        //             {
        //                 await mongoCollection.InsertOneAsync(BsonDocument.Parse(document.ToString()));
        //             }
        //
        //             break;
        //         }
        //         case JObject item:
        //             await mongoCollection.InsertOneAsync(BsonDocument.Parse(item.ToString()));
        //             break;
        //     }
        // }
    }
    
    public Task Cleanup(Sink data)
    {
        throw new NotImplementedException();
        // if (data.Type == TargetType.Mongodb && data.Cleanup.HasValues)
        // {
        //     var mongoCollection = _mongoDatabase.GetCollection<BsonDocument>(data.Destination);
        //
        //     switch (data.Cleanup)
        //     {
        //         case JArray array:
        //         {
        //             foreach (var document in array)
        //             {
        //                 await mongoCollection.DeleteManyAsync(BsonDocument.Parse(document.ToString()));
        //             }
        //
        //             break;
        //         }
        //         case JObject item:
        //             await mongoCollection.DeleteManyAsync(BsonDocument.Parse(item.ToString()));
        //             break;
        //     }
        // }
    }
    
    public Task<(bool, string)> Validate(Sink data)
    {
        throw new NotImplementedException();
        // if (data.Type != TargetType.Mongodb || !data.Expected.HasValues)
        //     return (false, "Not a valid test case, as no expected document found.");
        // var mongoCollection = _mongoDatabase.GetCollection<BsonDocument>(data.Destination);
        // switch (data.Expected)
        // {
        //     case JArray array:
        //     {
        //         foreach (var document in array)
        //         {
        //             var actual = (await mongoCollection.FindAsync(document.ToString())).FirstOrDefault();
        //             if (actual == null)
        //             {
        //                 return (false, "The expected document isn't present in mongodb.");
        //             }
        //         }
        //         break;
        //     }
        //     case JObject item:
        //     {
        //         var actual = (await mongoCollection.FindAsync(item.ToString())).FirstOrDefault();
        //         if (actual == null)
        //         {
        //             return (false, "The expected document isn't present in mongodb.");
        //         }
        //         break;
        //     }
        //     default:
        //         return (false, "Not a valid test case, as no expected document found.");
        // }
        //
        // return (true, "Test Passed.");
    }
}