using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Kafka.Connect.MongoDb.Models;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Linq;

namespace Kafka.Connect.MongoDb.Strategies;

public class ReadStrategy(ILogger<ReadStrategy> logger) : Strategy<FindModel<BsonDocument>>
{
    protected override Task<StrategyModel<FindModel<BsonDocument>>> BuildModels(string connector, ConnectRecord record)
    {
        throw new NotImplementedException();
    }

    protected override Task<StrategyModel<FindModel<BsonDocument>>> BuildModels(string connector, CommandRecord record)
    {
        using (logger.Track("Creating read models"))
        {
            var command = record.GetCommand<CommandConfig>();
            var model = new StrategyModel<FindModel<BsonDocument>> { Status = Status.Selecting };
            if (!record.IsChangeLog())
            {
                List<FilterDefinition<BsonDocument>> filters = [];
                var lookup = command.Filters?.Where(f => f.Value != null)
                    .Select(f => new {f.Key, Value =  f.Value is JsonElement je ? je.GetValue() : f.Value } ).ToList() ?? [];
                for (var i = 0; i < lookup.Count; i++)
                {
                    var rules = lookup.Take(i).Select(f => Builders<BsonDocument>.Filter.Eq(f.Key, f.Value)).ToList();
                    rules.Add(Builders<BsonDocument>.Filter.Gt(lookup.ElementAt(i).Key, lookup.ElementAt(i).Value));
                    filters.Add(Builders<BsonDocument>.Filter.And(rules));
                }

                model.Model = new FindModel<BsonDocument>
                {
                    Operation = "CHANGE",
                    Filter = filters.Count > 0
                        ? Builders<BsonDocument>.Filter.Or(filters)
                        : FilterDefinition<BsonDocument>.Empty,
                    Options = new FindOptions<BsonDocument>
                    {
                        Sort = command.Filters == null
                            ? null
                            : Builders<BsonDocument>.Sort.Combine(
                                command.Filters.Keys.Select(k => Builders<BsonDocument>.Sort.Ascending(k))),
                        BatchSize = record.BatchSize
                    }
                };
            }

            return Task.FromResult(model);
        }
    }
}
