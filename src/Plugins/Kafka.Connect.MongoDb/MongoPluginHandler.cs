using System;
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
            var strategy = connectPluginFactory.GetStrategy(connector, command);
            var commandConfig = command.GetCommand<CommandConfig>();
            
            // Check if this is a streams-based strategy
            if (strategy is Strategies.StreamsReadStrategy)
            {
                var watchModel = await strategy.Build<WatchModel>(connector, command);
                var changes = await mongoQueryRunner.WatchMany(watchModel, connector, taskId, commandConfig.Collection);
                
                return changes.Select(change => GetConnectRecordFromChange(change, command)).ToList();
            }
            else
            {
                var model = await strategy.Build<FindModel<BsonDocument>>(connector, command);
                var records = await mongoQueryRunner.ReadMany(model, connector, taskId, commandConfig.Collection);
                
                return records.Select(doc => GetConnectRecord(doc, command, model.Model.Operation)).ToList();
            }
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

    private static ConnectRecord GetConnectRecord(BsonValue bson, CommandRecord command, string operation)
    {
        var config = command.GetCommand<CommandConfig>();
        var json = JsonNode.Parse(JsonSerializer.Serialize(BsonTypeMapper.MapToDotNetValue(bson)))!;
        var value = operation switch
        {
            "CHANGE" => new JsonObject
            {
                { "id", JsonNode.Parse(json["_id"]?.ToString() ?? "{}") },
                { "operation", operation },
                { "timestamp", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() },
                { "before", null },
                { "after", json }
            }!,
            _ => JsonNode.Parse("[]")!
        };

        return new ConnectRecord(config.Topic, -1, -1)
        {
            Status = Status.Selected,
            Deserialized = new ConnectMessage<JsonNode>
            {
                Key = config.Keys != null
                    ? (value["after"]?.ToDictionary("after", true) ??
                       value["before"]?.ToDictionary("before", true))?
                            .Where(r => config.Keys.Contains(r.Key))
                            .ToDictionary(k => k.Key, v => v.Value)
                            .ToJson()
                    : null,
                Value = value
            }
        };
    }

    private static ConnectRecord GetConnectRecordFromChange(ChangeStreamDocument<BsonDocument> change, CommandRecord command)
    {
        var config = command.GetCommand<CommandConfig>();
        
        var operation = change.OperationType.ToString().ToUpperInvariant() switch
        {
            "INSERT" => "INSERT",
            "UPDATE" => "UPDATE",
            "REPLACE" => "UPDATE",
            "DELETE" => "DELETE",
            _ => "CHANGE"
        };

        JsonNode before = change.FullDocumentBeforeChange != null
            ? JsonNode.Parse(JsonSerializer.Serialize(BsonTypeMapper.MapToDotNetValue(change.FullDocumentBeforeChange)))
            : null;

        JsonNode after = change.FullDocument != null
            ? JsonNode.Parse(JsonSerializer.Serialize(BsonTypeMapper.MapToDotNetValue(change.FullDocument)))
            : null;

        var timestamp = change.ClusterTime?.Timestamp ?? (int)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        
        var value = new JsonObject
        {
            { "id", change.DocumentKey["_id"]?.ToString() ?? ""},
            { "operation", operation },
            { "timestamp", (long)timestamp * 1000000 }, 
            { "before", before },
            { "after", after },
            { "_resumeToken", JsonNode.Parse(change.ResumeToken.ToJson()) }
        };

        return new ConnectRecord(config.Topic, -1, -1)
        {
            Status = Status.Selected,
            Deserialized = new ConnectMessage<JsonNode>
            {
                Key = config.Keys != null
                    ? (value["after"]?.ToDictionary("after", true) ??
                       value["before"]?.ToDictionary("before", true))?
                            .Where(r => config.Keys.Contains(r.Key))
                            .ToDictionary(k => k.Key, v => v.Value)
                            .ToJson()
                    : null,
                Value = value
            }
        };
    }
}
