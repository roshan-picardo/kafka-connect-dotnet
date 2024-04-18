using System.Threading.Tasks;
using Kafka.Connect.MongoDb.Models;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Kafka.Connect.MongoDb.Strategies;

public class UpsertStrategy : ReadWriteStrategy<UpdateOneModel<BsonDocument>>
{
    private readonly ILogger<UpsertStrategy> _logger;
    private readonly IConfigurationProvider _configurationProvider;

    public UpsertStrategy(ILogger<UpsertStrategy> logger, IConfigurationProvider configurationProvider)
    {
        _logger = logger;
        _configurationProvider = configurationProvider;
    }
    
    protected override Task<StrategyModel<UpdateOneModel<BsonDocument>>> BuildSinkModels(string connector, ConnectRecord record)
    {
        using (_logger.Track("Creating upsert models"))
        {
            var condition = _configurationProvider.GetSinkConfigProperties<MongoSinkConfig>(connector).Condition;
            
            return Task.FromResult(new StrategyModel<UpdateOneModel<BsonDocument>>()
            {
                Status = SinkStatus.Updating,
                Model = new UpdateOneModel<BsonDocument>(
                    new BsonDocumentFilterDefinition<BsonDocument>(
                        BsonDocument.Parse(BuildCondition(condition.ToJsonString(), record.Deserialized.Value))),
                    new BsonDocumentUpdateDefinition<BsonDocument>(
                        new BsonDocument("$set", BsonDocument.Parse(record.Deserialized.Value.ToJsonString())))) { IsUpsert = true }
            });
        }
    }

    protected override Task<StrategyModel<UpdateOneModel<BsonDocument>>> BuildSourceModels(string connector, CommandRecord record)
    {
        throw new System.NotImplementedException();
    }
}