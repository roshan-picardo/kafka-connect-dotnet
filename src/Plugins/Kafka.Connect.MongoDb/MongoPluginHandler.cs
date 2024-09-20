using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.MongoDb.Collections;
using Kafka.Connect.MongoDb.Models;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Kafka.Connect.MongoDb;

public class MongoPluginHandler(
    IConfigurationProvider configurationProvider,
    IConnectPluginFactory connectPluginFactory,
    IMongoQueryRunner mongoQueryRunner,
    IMongoCommandHandler mongoCommandHandler,
    ILogger<MongoPluginHandler> logger)
    : PluginHandler(configurationProvider)
{
    private readonly IConfigurationProvider _configurationProvider = configurationProvider;
    public override Task Startup(string connector) => Task.CompletedTask;
    public override Task Purge(string connector) => Task.CompletedTask;

    public override async Task<IList<ConnectRecord>> Get(string connector, int taskId, CommandRecord command)
    {
        using (logger.Track("Getting batch of records"))
        {
            var model = await connectPluginFactory.GetStrategy(connector, command)
                .Build<FindModel<BsonDocument>>(connector, command);
            var commandConfig = command.GetCommand<CommandConfig>();

            var records = await mongoQueryRunner.ReadMany(model, connector, taskId, commandConfig.Collection);

            return records.Select(doc => new ConnectRecord(commandConfig.Topic, -1, -1)
                { Deserialized = GetConnectMessage(doc, commandConfig) }).ToList();
        }
    }

    public override async Task Put(IList<ConnectRecord> records, string connector, int taskId)
    {
        using (logger.Track("Putting batch of records"))
        {
            var parallelRetryOptions = _configurationProvider.GetParallelRetryOptions(connector);
            var models = new List<StrategyModel<WriteModel<BsonDocument>>>();
            await records.ForEachAsync(parallelRetryOptions, async cr =>
            {
                using (ConnectLog.TopicPartitionOffset(cr.Topic, cr.Partition, cr.Offset))
                {
                    if (cr is ConnectRecord { Sinking: true } record)
                    {
                        var model = await connectPluginFactory.GetStrategy(connector, record)
                            .Build<WriteModel<BsonDocument>>(connector, record);
                        models.Add(model);
                        record.Status = model.Status;
                    }
                }
            });

            await mongoQueryRunner.WriteMany(models.OrderBy(m => m.Topic)
                .ThenBy(m => m.Partition)
                .ThenBy(m => m.Offset)
                .SelectMany(m => m.Models).ToList(), connector, taskId);
        }
    }

    public override IDictionary<string, Command> Commands(string connector) => mongoCommandHandler.Get(connector);

    public override JsonNode NextCommand(CommandRecord command, List<ConnectRecord> records) =>
        mongoCommandHandler.Next(command,
            records.Where(r => r.Status is Status.Published or Status.Skipped)
                .Select(r => r.Deserialized).ToList());

    private static ConnectMessage<JsonNode> GetConnectMessage(BsonValue bson, CommandConfig command)
    {
        var record = JsonNode.Parse(JsonSerializer.Serialize(BsonTypeMapper.MapToDotNetValue(bson)));
        var flattened = record.ToDictionary();
        return new ConnectMessage<JsonNode>
        {
            Timestamp = (long)flattened[command.TimestampColumn],
            Key = JsonNode.Parse(JsonSerializer.Serialize(flattened
                .Where(r => command.KeyColumns?.Contains(r.Key) ?? false).ToDictionary(k => k.Key, v => v.Value))),
            Value = record
        };
    }
}
