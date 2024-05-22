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
        using (logger.Track("Putting batch of records"))
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
        var config = _configurationProvider.GetSourceConfigProperties<MongoSourceConfig>(connector);
        return config.Commands.ToDictionary(k=> k.Key, v => v.Value as Command);
    }

    public override CommandRecord GetUpdatedCommand(CommandRecord command, IList<(SinkStatus Status, JsonNode Key)> records)
    {
        var commandConfig = command.GetCommand<CommandConfig>();
        var jsonKeys = records.Where(r => r.Status is SinkStatus.Published or SinkStatus.Skipped or SinkStatus.Processed).Select(r=> r.Key).ToList();

        if (jsonKeys.Count > 0)
        {
            var maxTimestamp = jsonKeys.Select(k => k["Timestamp"]?.GetValue<long>() ?? 0).Max();
            
            var keys = jsonKeys.Where(k => k["Timestamp"]?.GetValue<long>() == maxTimestamp)
                .Select(k => k["Keys"]?.ToDictionary("Keys", true)).ToList();
            var sortedKeys = keys.OrderBy(_ => 1);
            foreach (var keyColumn in commandConfig.KeyColumns)
            {
                sortedKeys = sortedKeys.ThenBy(d => d[keyColumn]);
            }

            commandConfig.Timestamp = maxTimestamp;
            commandConfig.Keys = sortedKeys.LastOrDefault();
        }

        command.Command = commandConfig.ToJson();

        return command;
    }
    
    private static ConnectMessage<JsonNode> GetConnectMessage(BsonValue bson, CommandConfig command)
    {
        var record = JsonNode.Parse(JsonSerializer.Serialize(BsonTypeMapper.MapToDotNetValue(bson)));
        var flattened = record.ToDictionary();
        var commandKey = new
        {
            Timestamp = flattened[command.TimestampColumn],
            Keys = flattened.Where(r => command.KeyColumns?.Contains(r.Key) ?? false).ToDictionary(k => k.Key, v => v.Value)
        };
        return new ConnectMessage<JsonNode> { Key = JsonNode.Parse(JsonSerializer.Serialize(commandKey)), Value = record };
    }
}