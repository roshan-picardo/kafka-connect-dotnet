using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Kafka.Connect.MongoDb.Models;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Linq;

namespace Kafka.Connect.MongoDb.Strategies;

public class ReadStrategy(ILogger<ReadStrategy> logger, IConfigurationProvider configurationProvider)
    : Strategy<object>
{
    protected override Task<StrategyModel<object>> BuildModels(string connector, ConnectRecord record)
    {
        throw new NotImplementedException();
    }

    protected override Task<StrategyModel<object>> BuildModels(string connector, CommandRecord record)
    {
        using (logger.Track("Creating read models"))
        {
            var pluginConfig = configurationProvider.GetPluginConfig<PluginConfig>(connector);
            var command = record.GetCommand<CommandConfig>();
            
            // Use Change Streams if enabled
            if (pluginConfig.UseChangeStreams)
            {
                return Task.FromResult(BuildChangeStreamModel(command, record, pluginConfig));
            }
            
            // Fall back to traditional Find-based approach
            return Task.FromResult(BuildFindModel(command, record));
        }
    }

    private StrategyModel<object> BuildChangeStreamModel(CommandConfig command, CommandRecord record, PluginConfig pluginConfig)
    {
        var model = new StrategyModel<object> { Status = Status.Selecting };
        
        var changeStreamOptions = new ChangeStreamOptions
        {
            FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
            MaxAwaitTime = TimeSpan.FromMilliseconds(pluginConfig.ChangeStreamMaxAwaitTimeMs)
        };

        // Set resume token if available
        if (!string.IsNullOrEmpty(command.ResumeToken))
        {
            try
            {
                var resumeTokenDoc = BsonDocument.Parse(command.ResumeToken);
                changeStreamOptions.ResumeAfter = resumeTokenDoc;
                logger.Debug("Resuming change stream from token", new { ResumeToken = command.ResumeToken });
            }
            catch (Exception ex)
            {
                logger.Warning("Failed to parse resume token, starting from latest", ex);
            }
        }
        else
        {
            // Set start mode based on configuration
            switch (pluginConfig.ChangeStreamStartMode?.ToLowerInvariant())
            {
                case "earliest":
                    changeStreamOptions.StartAtOperationTime = new BsonTimestamp(0);
                    break;
                case "timestamp" when pluginConfig.ChangeStreamStartTimestamp.HasValue:
                    changeStreamOptions.StartAtOperationTime = new BsonTimestamp(pluginConfig.ChangeStreamStartTimestamp.Value);
                    break;
                default: // "latest" or null
                    // Don't set any start option - will start from latest
                    break;
            }
        }

        // Build pipeline for filtering if needed
        var pipeline = new EmptyPipelineDefinition<ChangeStreamDocument<BsonDocument>>()
            .As<ChangeStreamDocument<BsonDocument>, ChangeStreamDocument<BsonDocument>, ChangeStreamDocument<BsonDocument>>();

        // Add match stage for operation types if needed
        var matchStages = new List<BsonDocument>();
        
        // Filter by operation types (insert, update, replace, delete)
        var operationTypes = new BsonArray { "insert", "update", "replace", "delete" };
        matchStages.Add(new BsonDocument("$match", new BsonDocument("operationType", new BsonDocument("$in", operationTypes))));

        if (matchStages.Any())
        {
            pipeline = PipelineDefinition<ChangeStreamDocument<BsonDocument>, ChangeStreamDocument<BsonDocument>>
                .Create(matchStages);
        }

        model.Model = new ChangeStreamModel<BsonDocument>
        {
            Operation = "STREAM",
            Options = changeStreamOptions,
            Pipeline = pipeline,
            MaxAwaitTimeMs = pluginConfig.ChangeStreamMaxAwaitTimeMs,
            UseChangeStreams = true
        };

        return model;
    }

    private StrategyModel<object> BuildFindModel(CommandConfig command, CommandRecord record)
    {
        var model = new StrategyModel<object> { Status = Status.Selecting };
        
        if (!record.IsChangeLog())
        {
            List<FilterDefinition<BsonDocument>> filters = [];
            var lookup = command.Filters?.Where(f => f.Value != null)
                .Select(f => new {f.Key, Value =  f.Value is JsonElement je ? je.GetValue() : f.Value } ).ToList() ?? [];
            
            for (var i = 0; i < lookup.Count; i++)
            {
                var rules = lookup.Take(i).Select(f => Builders<BsonDocument>.Filter.Eq(f.Key, f.Value)).ToList();
                rules.Add(Builders<BsonDocument>.Filter.Gt(lookup.ElementAt(i).Key, lookup.ElementAt(i).Value));
                filters.Add(Builders<BsonDocument>.Filter.And(rules));
            }

            model.Model = new FindModel<BsonDocument>
            {
                Operation = "CHANGE",
                Filter = filters.Count > 0
                    ? Builders<BsonDocument>.Filter.Or(filters)
                    : FilterDefinition<BsonDocument>.Empty,
                Options = new FindOptions<BsonDocument>
                {
                    Sort = command.Filters == null
                        ? null
                        : Builders<BsonDocument>.Sort.Combine(
                            command.Filters.Keys.Select(k => Builders<BsonDocument>.Sort.Ascending(k))),
                    BatchSize = record.BatchSize
                }
            };
        }

        return model;
    }
}
