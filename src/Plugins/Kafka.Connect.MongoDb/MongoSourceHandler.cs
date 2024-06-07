using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Kafka.Connect.MongoDb.Collections;
using Kafka.Connect.MongoDb.Models;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Logging;
using MongoDB.Bson;

namespace Kafka.Connect.MongoDb;

public class MongoSourceHandler(
    IConfigurationProvider configurationProvider,
    IReadWriteStrategyProvider readWriteStrategyProvider,
    IMongoQueryRunner mongoQueryRunner,
    ILogger<MongoSourceHandler> logger)
    : SourceHandler(configurationProvider, readWriteStrategyProvider)
{
    private readonly IConfigurationProvider _configurationProvider = configurationProvider;

    public override async Task<IList<ConnectRecord>> Get(string connector, int taskId, CommandRecord command)
    {
        using (logger.Track("Getting batch of records"))
        {
            var model = await GetReadWriteStrategy(connector, command).Build<FindModel<BsonDocument>>(connector, command);
            var commandConfig = command.GetCommand<CommandConfig>();

            var records = await mongoQueryRunner.ReadMany(model, connector, taskId, commandConfig.Collection);
            
            return records.Select(doc => new ConnectRecord(commandConfig.Topic, -1, -1)
                { Deserialized = GetConnectMessage(doc, commandConfig) }).ToList();
        }
    }

    public override IDictionary<string, Command> GetCommands(string connector)
    {
        var config = _configurationProvider.GetPluginConfig<SourceConfig>(connector);
        return config.Commands.ToDictionary(k=> k.Key, v => v.Value as Command);
    }

    public override CommandRecord GetUpdatedCommand(CommandRecord command, IList<ConnectMessage<JsonNode>> records)
    {
        var commandConfig = command.GetCommand<CommandConfig>();
        if (records.Any())
        {
            var maxTimestamp = records.Max(m => m.Timestamp);
            var keys = records.Where(m => m.Timestamp == maxTimestamp).Select(m => m.Key.ToDictionary())
                .OrderBy(_ => 1);
            keys = commandConfig.KeyColumns.Aggregate(keys, (current, keyColumn) => current.ThenBy(d => d[keyColumn]));
            commandConfig.Timestamp = maxTimestamp;
            commandConfig.Keys = keys.LastOrDefault();
        }
        command.Command = commandConfig.ToJson();
        return command;
    }
    private static ConnectMessage<JsonNode> GetConnectMessage(BsonValue bson, CommandConfig command)
    {
        var record = JsonNode.Parse(JsonSerializer.Serialize(BsonTypeMapper.MapToDotNetValue(bson)));
        var flattened = record.ToDictionary();
        return new ConnectMessage<JsonNode>
        {
            Timestamp = (long)flattened[command.TimestampColumn], 
            Key = JsonNode.Parse(JsonSerializer.Serialize(flattened.Where(r => command.KeyColumns?.Contains(r.Key) ?? false).ToDictionary(k => k.Key, v => v.Value))),
            Value = record
        };
    }
}