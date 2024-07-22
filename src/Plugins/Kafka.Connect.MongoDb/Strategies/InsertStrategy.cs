using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Kafka.Connect.MongoDb.Strategies;

public class InsertStrategy(ILogger<InsertStrategy> logger) : Strategy<InsertOneModel<BsonDocument>>
{
    protected override Task<StrategyModel<InsertOneModel<BsonDocument>>> BuildModels(string connector, ConnectRecord record)
    {
        using (logger.Track("Creating insert write models"))
        {
            return Task.FromResult(new StrategyModel<InsertOneModel<BsonDocument>>()
            {
                Status = SinkStatus.Inserting,
                Model = new InsertOneModel<BsonDocument>(BsonDocument.Parse(record.Deserialized.Value.ToJsonString()))
            });
        }
    }

    protected override Task<StrategyModel<InsertOneModel<BsonDocument>>> BuildModels(string connector, CommandRecord record)
    {
        throw new NotImplementedException();
    }
}