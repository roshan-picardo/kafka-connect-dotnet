using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.MongoDb.Models;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Kafka.Connect.MongoDb.Strategies;

public class ReadStrategy : ReadWriteStrategy<FindModel<BsonDocument>>
{
    private readonly ILogger<ReadStrategy> _logger;

    public ReadStrategy(ILogger<ReadStrategy> logger)
    {
        _logger = logger;
    }

    protected override Task<StrategyModel<FindModel<BsonDocument>>> BuildSinkModels(string connector, ConnectRecord record)
    {
        throw new NotImplementedException();
    }

    protected override Task<StrategyModel<FindModel<BsonDocument>>> BuildSourceModels(string connector, CommandRecord record)
    {
        using (_logger.Track("Creating read models"))
        {
            var command = record.GetCommand<CommandConfig>();

            var buildFilter = Builders<BsonDocument>.Filter;
            var buildOrder = Builders<BsonDocument>.Sort;
            var filters = new Dictionary<string, object> { { command.TimestampColumn, command.Timestamp } };
            var filter = buildFilter.Empty;
            var sort = buildOrder.Ascending(command.TimestampColumn);

            if (command.KeyColumns != null)
            {
                foreach (var keyColumn in command.KeyColumns)
                {
                    if (command.Keys?.TryGetValue(keyColumn, out var value) ?? false)
                    {
                        filters.Add(keyColumn, value is JsonElement je ? je.GetValue() : value);
                    }

                    sort.Ascending(keyColumn);
                }
            }

            var index = filters.Count;

            foreach (var f in filters)
            {
                filter &= --index == 0 ? buildFilter.Gt(f.Key, f.Value) : buildFilter.Gte(f.Key, f.Value);
            }

            return Task.FromResult(new StrategyModel<FindModel<BsonDocument>>
            {
                Status = SinkStatus.Selecting,
                Model = new FindModel<BsonDocument>
                {
                    Filter = filter,
                    Options = new FindOptions<BsonDocument> { Sort = sort, Limit =  record.BatchSize }
                }
            });
            
        }
    }
}