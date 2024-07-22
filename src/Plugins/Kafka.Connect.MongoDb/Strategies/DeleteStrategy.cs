using System;
using System.Threading.Tasks;
using Kafka.Connect.MongoDb.Models;
using Kafka.Connect.Plugin.Extensions;
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
    protected override Task<StrategyModel<DeleteOneModel<BsonDocument>>> BuildModels(string connector, ConnectRecord record)
    {
        using (logger.Track("Creating delete models"))
        {
            var config = configurationProvider.GetPluginConfig<PluginConfig>(connector);
            var filter = BsonDocument.Parse(BuildCondition(config.Filter.ToString(), record.Deserialized.Value.ToDictionary()));
            return Task.FromResult(new StrategyModel<DeleteOneModel<BsonDocument>>()
            {
                Status = SinkStatus.Deleting,
                Model = new DeleteOneModel<BsonDocument>(
                    new BsonDocumentFilterDefinition<BsonDocument>(filter))
            });
        }
    }

    protected override Task<StrategyModel<DeleteOneModel<BsonDocument>>> BuildModels(string connector, CommandRecord record) 
        => throw new NotImplementedException();
}