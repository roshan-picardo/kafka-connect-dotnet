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
            var pluginConfig = _configurationProvider.GetPluginConfig<PluginConfig>(connector);
            var commandConfig = command.GetCommand<CommandConfig>();

            // Use Change Streams if enabled
            if (pluginConfig.UseChangeStreams)
            {
                var strategyModel = await connectPluginFactory.GetStrategy(connector, command)
                    .Build<object>(connector, command);

                // Cast the model to ChangeStreamModel
                var changeStreamModel = new StrategyModel<ChangeStreamModel<BsonDocument>>
                {
                    Status = strategyModel.Status,
                    Model = (ChangeStreamModel<BsonDocument>)strategyModel.Model,
                    Topic = strategyModel.Topic,
                    Partition = strategyModel.Partition,
                    Offset = strategyModel.Offset
                };

                var (changeDocuments, resumeToken) = await mongoQueryRunner.ReadChangeStream(
                    changeStreamModel,
                    connector,
                    taskId,
                    commandConfig.Collection,
                    command.BatchSize);

                var records = changeDocuments
                    .Select(changeDoc => GetConnectRecordFromChangeStream(changeDoc, command, resumeToken))
                    .ToList();

                return records;
            }
            else
            {
                // Traditional Find-based approach
                var strategyModel = await connectPluginFactory.GetStrategy(connector, command)
                    .Build<object>(connector, command);

                // Cast the model to FindModel
                var findModel = new StrategyModel<FindModel<BsonDocument>>
                {
                    Status = strategyModel.Status,
                    Model = (FindModel<BsonDocument>)strategyModel.Model,
                    Topic = strategyModel.Topic,
                    Partition = strategyModel.Partition,
                    Offset = strategyModel.Offset
                };

                var documents = await mongoQueryRunner.ReadMany(findModel, connector, taskId, commandConfig.Collection);

                return documents.Select(doc => GetConnectRecord(doc, command, findModel.Model.Operation)).ToList();
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

    public override JsonNode NextCommand(CommandRecord command, List<ConnectRecord> records)
    {
        var pluginConfig = _configurationProvider.GetPluginConfig<PluginConfig>(connector: command.Connector);
        
        // For Change Streams, update the resume token from the last record
        if (pluginConfig.UseChangeStreams && records.Any())
        {
            var lastRecord = records.Last();
            if (lastRecord.Deserialized?.Value is JsonObject valueObj &&
                valueObj.TryGetPropertyValue("resumeToken", out var tokenNode))
            {
                var commandConfig = command.GetCommand<CommandConfig>();
                commandConfig.ResumeToken = tokenNode?.ToString();
                return commandConfig.ToJson();
            }
        }
        
        return mongoCommandHandler.Next(command,
            records.Where(r => r.Status is Status.Published or Status.Skipped)
                .Select(r => r.Deserialized).ToList());
    }

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

    private static ConnectRecord GetConnectRecordFromChangeStream(
        ChangeStreamDocument<BsonDocument> changeDoc,
        CommandRecord command,
        string resumeToken)
    {
        var config = command.GetCommand<CommandConfig>();
        
        // Map MongoDB change stream operation types to our operation types
        var operation = changeDoc.OperationType.ToString().ToUpperInvariant();
        
        // Get the document data
        BsonDocument beforeDoc = null;
        BsonDocument afterDoc = null;
        
        switch (changeDoc.OperationType)
        {
            case ChangeStreamOperationType.Insert:
                afterDoc = changeDoc.FullDocument;
                operation = "INSERT";
                break;
            case ChangeStreamOperationType.Update:
            case ChangeStreamOperationType.Replace:
                afterDoc = changeDoc.FullDocument;
                operation = "UPDATE";
                break;
            case ChangeStreamOperationType.Delete:
                // For deletes, we only have the _id in DocumentKey
                beforeDoc = new BsonDocument { { "_id", changeDoc.DocumentKey["_id"] } };
                operation = "DELETE";
                break;
        }

        // Convert to JSON
        var afterJson = afterDoc != null
            ? JsonNode.Parse(JsonSerializer.Serialize(BsonTypeMapper.MapToDotNetValue(afterDoc)))
            : null;
        var beforeJson = beforeDoc != null
            ? JsonNode.Parse(JsonSerializer.Serialize(BsonTypeMapper.MapToDotNetValue(beforeDoc)))
            : null;

        var documentId = changeDoc.DocumentKey?["_id"] != null
            ? JsonNode.Parse(JsonSerializer.Serialize(BsonTypeMapper.MapToDotNetValue(changeDoc.DocumentKey["_id"])))
            : null;

        var value = new JsonObject
        {
            { "id", documentId },
            { "operation", operation },
            { "timestamp", changeDoc.ClusterTime?.Timestamp ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() },
            { "before", beforeJson },
            { "after", afterJson },
            { "resumeToken", resumeToken }  // Store resume token for next command
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
