using System;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using Kafka.Connect.DynamoDb.Models;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;

namespace Kafka.Connect.DynamoDb.Strategies;

public class StreamReadStrategy(ILogger<StreamReadStrategy> logger) : Strategy<StreamModel>
{
    private static readonly DateTime ConnectorStartTime = DateTime.UtcNow;
    
    protected override Task<StrategyModel<StreamModel>> BuildModels(string connector, ConnectRecord record)
    {
        throw new NotImplementedException();
    }

    protected override Task<StrategyModel<StreamModel>> BuildModels(string connector, CommandRecord record)
    {
        using (logger.Track("Creating stream read models"))
        {
            var command = record.GetCommand<CommandConfig>();
            var model = new StrategyModel<StreamModel> { Status = Status.Selecting };
            
            if (!record.IsChangeLog())
            {
                // Determine shard iterator type
                // Priority order:
                // 1. Resume from sequence number (for fault tolerance)
                // 2. Use configured iterator type
                // 3. Default to LATEST (only new changes)
                
                string shardIteratorType;
                string sequenceNumber = null;
                
                if (!string.IsNullOrEmpty(command.SequenceNumber))
                {
                    // Resume from last known position
                    shardIteratorType = "AFTER_SEQUENCE_NUMBER";
                    sequenceNumber = command.SequenceNumber;
                    logger.Debug("Resuming stream from sequence number", new { SequenceNumber = sequenceNumber });
                }
                else if (!string.IsNullOrEmpty(command.ShardIteratorType))
                {
                    // Use configured iterator type
                    shardIteratorType = command.ShardIteratorType;
                    logger.Debug("Using configured shard iterator type", new { ShardIteratorType = shardIteratorType });
                }
                else if (command.Timestamp > 0)
                {
                    // Start from timestamp
                    shardIteratorType = "AT_TIMESTAMP";
                    logger.Debug("Starting stream from timestamp", new { Timestamp = command.Timestamp });
                }
                else
                {
                    // Default: start from latest (only capture new changes)
                    shardIteratorType = "LATEST";
                    logger.Debug("Starting stream from LATEST");
                }

                model.Model = new StreamModel
                {
                    Operation = "STREAM",
                    TableName = command.TableName,
                    StreamArn = command.StreamArn,
                    ShardIteratorType = shardIteratorType,
                    SequenceNumber = sequenceNumber,
                    Request = new GetRecordsRequest
                    {
                        Limit = record.BatchSize
                    }
                };
            }

            return Task.FromResult(model);
        }
    }
}
