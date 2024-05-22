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

namespace Kafka.Connect.MongoDb.Strategies;

public class ReadStrategy(ILogger<ReadStrategy> logger) : ReadWriteStrategy<FindModel<BsonDocument>>
{
    protected override Task<StrategyModel<FindModel<BsonDocument>>> BuildSinkModels(string connector, ConnectRecord record)
    {
        throw new NotImplementedException();
    }

    protected override Task<StrategyModel<FindModel<BsonDocument>>> BuildSourceModels(string connector, CommandRecord record)
    {
        using (logger.Track("Creating read models"))
        {
            var command = record.GetCommand<CommandConfig>();
            var buildOrder = Builders<BsonDocument>.Sort;
            var sort = buildOrder.Ascending(command.TimestampColumn);
            var filters = new List<List<FilterDefinition<BsonDocument>>>
            {
                new() { Builders<BsonDocument>.Filter.Gt(command.TimestampColumn, command.Timestamp) },
                new() { Builders<BsonDocument>.Filter.Eq(command.TimestampColumn, command.Timestamp) }
            };
            if (command.KeyColumns != null)
            {
                const int index = 1;
                foreach (var keyColumn in command.KeyColumns)
                {
                    if (command.Keys?.TryGetValue(keyColumn, out var obj) ?? false)
                    {
                        var value = obj is JsonElement je ? je.GetValue() : obj;
                        filters.Add([..filters[index], Builders<BsonDocument>.Filter.Eq(keyColumn, value)]);
                        filters[index].Add(Builders<BsonDocument>.Filter.Gt(keyColumn, value));
                    }

                    sort.Ascending(keyColumn);
                }
            }

            filters.RemoveAt(filters.Count - 1);

            return Task.FromResult(new StrategyModel<FindModel<BsonDocument>>
            {
                Status = SinkStatus.Selecting,
                Model = new FindModel<BsonDocument>
                {
                    Filter =
                        Builders<BsonDocument>.Filter.Or(filters.Select(f => Builders<BsonDocument>.Filter.And(f))),
                    Options = new FindOptions<BsonDocument> { Sort = sort, Limit = record.BatchSize }
                }
            });
        }
    }
}