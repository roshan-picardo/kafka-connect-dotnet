using System;
using System.Threading.Tasks;
using Kafka.Connect.MongoDb.Models;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Kafka.Connect.MongoDb.Strategies;

public class StreamsReadStrategy(ILogger<StreamsReadStrategy> logger) : Strategy<WatchModel>
{
    private static readonly DateTime ConnectorStartTime = DateTime.UtcNow;
    
    protected override Task<StrategyModel<WatchModel>> BuildModels(string connector, ConnectRecord record)
    {
        throw new NotImplementedException();
    }

    protected override Task<StrategyModel<WatchModel>> BuildModels(string connector, CommandRecord record)
    {
        using (logger.Track("Creating streams read models"))
        {
            var command = record.GetCommand<CommandConfig>();
            var model = new StrategyModel<WatchModel> { Status = Status.Selecting };
            
            if (!record.IsChangeLog())
            {
                var options = new ChangeStreamOptions
                {
                    FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                    FullDocumentBeforeChange = ChangeStreamFullDocumentBeforeChangeOption.WhenAvailable,
                    BatchSize = record.BatchSize,
                    // Set MaxAwaitTime to prevent blocking indefinitely
                    // This allows the cursor to return quickly if no changes are available
                    MaxAwaitTime = TimeSpan.FromMilliseconds(100)
                };

                // Priority order for determining where to start the change stream:
                // 1. Resume token (highest priority - for fault tolerance)
                // 2. Explicit timestamp from command configuration
                // 3. Connector start time (default - captures changes from when connector started)
                
                if (command.Filters != null && command.Filters.TryGetValue("_resumeToken", out var resumeTokenValue))
                {
                    // Resume from last known position (for fault tolerance)
                    if (resumeTokenValue != null)
                    {
                        var resumeToken = BsonDocument.Parse(resumeTokenValue.ToString());
                        options.ResumeAfter = resumeToken;
                        logger.Debug("Resuming change stream from token", new { ResumeToken = resumeToken });
                    }
                }
                else if (command.Timestamp > 0)
                {
                    // Start from explicitly configured timestamp
                    var startTime = BsonTimestamp.Create(command.Timestamp);
                    options.StartAtOperationTime = startTime;
                    logger.Debug("Starting change stream from configured timestamp", new { Timestamp = command.Timestamp });
                }
                else
                {
                    // Default: start from connector start time
                    // This ensures we capture all changes from when the connector was started
                    var startTimestamp = new DateTimeOffset(ConnectorStartTime).ToUnixTimeSeconds();
                    var startTime = new BsonTimestamp((int)startTimestamp, 0);
                    options.StartAtOperationTime = startTime;
                    logger.Debug("Starting change stream from connector start time", new { StartTime = ConnectorStartTime, Timestamp = startTimestamp });
                }

                model.Model = new WatchModel
                {
                    Operation = "STREAM",
                    Options = options
                };
            }

            return Task.FromResult(model);
        }
    }
}
