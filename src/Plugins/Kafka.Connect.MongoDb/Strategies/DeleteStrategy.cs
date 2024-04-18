using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.MongoDb.Models;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Kafka.Connect.MongoDb.Strategies;

public class DeleteStrategy : ReadWriteStrategy<DeleteOneModel<BsonDocument>>
{
    private readonly ILogger<DeleteStrategy> _logger;
    private readonly IConfigurationProvider _configurationProvider;

    public DeleteStrategy(ILogger<DeleteStrategy> logger, IConfigurationProvider configurationProvider)
    {
        _logger = logger;
        _configurationProvider = configurationProvider;
    }
    
    protected override Task<StrategyModel<DeleteOneModel<BsonDocument>>> BuildSinkModels(string connector, ConnectRecord record)
    {
        using (_logger.Track("Creating delete models"))
        {
            var condition = _configurationProvider.GetSinkConfigProperties<MongoSinkConfig>(connector).Condition;
            return BuildDeleteModels(condition, record.Deserialized);
        }
    }

    protected override Task<StrategyModel<DeleteOneModel<BsonDocument>>> BuildSourceModels(string connector, CommandRecord record)
    {
        using (_logger.Track("Creating delete models"))
        {
            var condition = _configurationProvider.GetSinkConfigProperties<MongoSinkConfig>(connector).Condition;
            return BuildDeleteModels(condition, new ConnectMessage<JsonNode>());
        }       
    }

    private Task<StrategyModel<DeleteOneModel<BsonDocument>>> BuildDeleteModels(
        JsonNode condition,
        ConnectMessage<JsonNode> message) =>
        Task.FromResult(new StrategyModel<DeleteOneModel<BsonDocument>>()
        {
            Status = SinkStatus.Deleting,
            Model = new DeleteOneModel<BsonDocument>(
                new BsonDocumentFilterDefinition<BsonDocument>(
                    BsonDocument.Parse(BuildCondition(condition.ToJsonString(), message.Value))))
        });
}