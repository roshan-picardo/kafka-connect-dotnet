using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using Kafka.Connect.DynamoDb.Collections;
using Kafka.Connect.DynamoDb.Models;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;

namespace Kafka.Connect.DynamoDb;

public class DynamoDbPluginHandler(
    IConfigurationProvider configurationProvider,
    IConnectPluginFactory connectPluginFactory,
    IDynamoDbQueryRunner dynamoDbQueryRunner,
    IDynamoDbCommandHandler dynamoDbCommandHandler,
    ILogger<DynamoDbPluginHandler> logger)
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
            
            var model = await strategy.Build<ScanModel>(connector, command);
            var items = await dynamoDbQueryRunner.ScanMany(model, connector, taskId, commandConfig.TableName);
            
            return items.Select(item => GetConnectRecord(item, command, model.Model.Operation)).ToList();
        }
    }

    public override async Task Put(IList<ConnectRecord> records, string connector, int taskId)
    {
        using (logger.Track("Putting batch of records"))
        {
            var parallelRetryOptions = _configurationProvider.GetParallelRetryOptions(connector);
            var models = new List<StrategyModel<WriteRequest>>();
            
            await records.ForEachAsync(parallelRetryOptions, async cr =>
            {
                using (ConnectLog.TopicPartitionOffset(cr.Topic, cr.Partition, cr.Offset))
                {
                    if (cr is ConnectRecord { Sinking: true } record)
                    {
                        var model = await connectPluginFactory.GetStrategy(connector, record)
                            .Build<WriteRequest>(connector, record);
                        models.Add(model);
                        record.Status = model.Status;
                    }
                }
            });

            var config = _configurationProvider.GetPluginConfig<PluginConfig>(connector);
            var writeRequests = models.OrderBy(m => m.Topic)
                .ThenBy(m => m.Partition)
                .ThenBy(m => m.Offset)
                .Select(m => m.Model)
                .ToList();
                
            await dynamoDbQueryRunner.WriteMany(writeRequests, connector, taskId, config.TableName);
        }
    }

    public override IDictionary<string, Command> Commands(string connector) => 
        dynamoDbCommandHandler.Get(connector);

    public override JsonNode NextCommand(CommandRecord command, List<ConnectRecord> records) =>
        dynamoDbCommandHandler.Next(command,
            records.Where(r => r.Status is Status.Published or Status.Skipped)
                .Select(r => r.Deserialized).ToList());

    private static ConnectRecord GetConnectRecord(Dictionary<string, AttributeValue> item, CommandRecord command, string operation)
    {
        var config = command.GetCommand<CommandConfig>();
        var json = ConvertAttributeValuesToJson(item);
        
        var value = operation switch
        {
            "SCAN" => new JsonObject
            {
                { "operation", "READ" },
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
                    ? json.ToDictionary()
                        .Where(r => config.Keys.Contains(r.Key))
                        .ToDictionary(k => k.Key, v => v.Value)
                        .ToJson()
                    : null,
                Value = value
            }
        };
    }

    private static JsonNode ConvertAttributeValuesToJson(Dictionary<string, AttributeValue> attributes)
    {
        var jsonObject = new JsonObject();
        
        foreach (var kvp in attributes)
        {
            jsonObject[kvp.Key] = ConvertAttributeValueToJsonNode(kvp.Value);
        }
        
        return jsonObject;
    }

    private static JsonNode ConvertAttributeValueToJsonNode(AttributeValue attributeValue)
    {
        if (attributeValue.S != null)
            return JsonValue.Create(attributeValue.S);
        
        if (attributeValue.N != null)
            return JsonValue.Create(double.Parse(attributeValue.N));
        
        if (attributeValue.BOOL)
            return JsonValue.Create(attributeValue.BOOL);
        
        if (attributeValue.NULL)
            return null;
        
        if (attributeValue.L != null && attributeValue.L.Any())
        {
            var array = new JsonArray();
            foreach (var item in attributeValue.L)
            {
                array.Add(ConvertAttributeValueToJsonNode(item));
            }
            return array;
        }
        
        if (attributeValue.M != null && attributeValue.M.Any())
        {
            var obj = new JsonObject();
            foreach (var kvp in attributeValue.M)
            {
                obj[kvp.Key] = ConvertAttributeValueToJsonNode(kvp.Value);
            }
            return obj;
        }
        
        if (attributeValue.SS != null && attributeValue.SS.Any())
        {
            var array = new JsonArray();
            foreach (var item in attributeValue.SS)
            {
                array.Add(JsonValue.Create(item));
            }
            return array;
        }
        
        if (attributeValue.NS != null && attributeValue.NS.Any())
        {
            var array = new JsonArray();
            foreach (var item in attributeValue.NS)
            {
                array.Add(JsonValue.Create(double.Parse(item)));
            }
            return array;
        }
        
        return null;
    }
}
