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

public class DeleteStrategy(ILogger<DeleteStrategy> logger, IConfigurationProvider configurationProvider)
    : Strategy<DeleteOneModel<BsonDocument>>
{
    protected override Task<StrategyModel<DeleteOneModel<BsonDocument>>> BuildSinkModels(string connector, ConnectRecord record)
    {
        using (logger.Track("Creating delete models"))
        {
            var condition = configurationProvider.GetPluginConfig<SinkConfig>(connector).Condition;
            return BuildDeleteModels(condition, record.Deserialized);
        }
    }

    protected override Task<StrategyModel<DeleteOneModel<BsonDocument>>> BuildSourceModels(string connector, CommandRecord record)
    {
        using (logger.Track("Creating delete models"))
        {
            var condition = configurationProvider.GetPluginConfig<SinkConfig>(connector).Condition; // TODO: Must change to SourceConfig
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