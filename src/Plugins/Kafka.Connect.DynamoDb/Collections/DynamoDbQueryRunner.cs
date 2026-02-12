using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Kafka.Connect.DynamoDb.Models;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;

namespace Kafka.Connect.DynamoDb.Collections;

public class DynamoDbQueryRunner(
    ILogger<DynamoDbQueryRunner> logger,
    IDynamoDbClientProvider dynamoDbClientProvider,
    IConfigurationProvider configurationProvider)
    : IDynamoDbQueryRunner
{
    public async Task WriteMany(IList<WriteRequest> requests, string connector, int taskId, string tableName)
    {
        using (logger.Track("Writing requests to DynamoDB"))
        {
            var sinkConfig = configurationProvider.GetPluginConfig<PluginConfig>(connector);
            if (requests == null || !requests.Any())
            {
                return;
            }

            using (logger.Track($"Writing to table: {tableName}"))
            {
                try
                {
                    var client = dynamoDbClientProvider.GetDynamoDbClient(connector, taskId);
                    
                    // DynamoDB BatchWriteItem has a limit of 25 items per request
                    var batches = requests.Select((item, index) => new { item, index })
                        .GroupBy(x => x.index / 25)
                        .Select(g => g.Select(x => x.item).ToList())
                        .ToList();

                    foreach (var batch in batches)
                    {
                        var batchRequest = new BatchWriteItemRequest
                        {
                            RequestItems = new Dictionary<string, List<WriteRequest>>
                            {
                                { tableName, batch }
                            }
                        };

                        var response = await client.BatchWriteItemAsync(batchRequest);
                        
                        logger.Debug("Batch written successfully to DynamoDB.",
                            new
                            {
                                RequestCount = batch.Count,
                                UnprocessedItems = response.UnprocessedItems.Count
                            });

                        // Handle unprocessed items with exponential backoff
                        var retryCount = 0;
                        while (response.UnprocessedItems.Count > 0 && retryCount < 5)
                        {
                            await Task.Delay((int)Math.Pow(2, retryCount) * 100);
                            var retryRequest = new BatchWriteItemRequest
                            {
                                RequestItems = response.UnprocessedItems
                            };
                            response = await client.BatchWriteItemAsync(retryRequest);
                            retryCount++;
                        }
                    }
                }
                catch (ProvisionedThroughputExceededException ex)
                {
                    throw new ConnectRetriableException(ex.Message, ex);
                }
                catch (Amazon.DynamoDBv2.AmazonDynamoDBException ex)
                {
                    throw new ConnectRetriableException(ex.Message, ex);
                }
            }
        }
    }

    public async Task<IList<Dictionary<string, AttributeValue>>> ScanMany(
        StrategyModel<ScanModel> model,
        string connector,
        int taskId,
        string tableName)
    {
        using (logger.Track("Scanning DynamoDB table"))
        {
            var client = dynamoDbClientProvider.GetDynamoDbClient(connector, taskId);
            var items = new List<Dictionary<string, AttributeValue>>();

            using (logger.Track($"Scanning table: {tableName}"))
            {
                var response = await client.ScanAsync(model.Model.Request);
                items.AddRange(response.Items);

                logger.Debug("Scan completed", new { Count = items.Count });
                return items;
            }
        }
    }

    public async Task<IList<Dictionary<string, AttributeValue>>> QueryMany(
        StrategyModel<QueryModel> model,
        string connector,
        int taskId,
        string tableName)
    {
        using (logger.Track("Querying DynamoDB table"))
        {
            var client = dynamoDbClientProvider.GetDynamoDbClient(connector, taskId);
            var items = new List<Dictionary<string, AttributeValue>>();

            using (logger.Track($"Querying table: {tableName}"))
            {
                var response = await client.QueryAsync(model.Model.Request);
                items.AddRange(response.Items);

                logger.Debug("Query completed", new { Count = items.Count });
                return items;
            }
        }
    }

    public async Task<(IList<Record> Records, string NextShardIterator)> ReadStream(
        StrategyModel<StreamModel> model,
        string connector,
        int taskId)
    {
        using (logger.Track("Reading DynamoDB stream"))
        {
            var client = dynamoDbClientProvider.GetStreamsClient(connector, taskId);
            
            if (client == null)
            {
                throw new InvalidOperationException("DynamoDB Streams client is not available");
            }
            
            var streamModel = model.Model;

            // Get stream ARN if not provided
            if (string.IsNullOrEmpty(streamModel.StreamArn) && !string.IsNullOrEmpty(streamModel.TableName))
            {
                using (logger.Track($"Fetching stream ARN for table: {streamModel.TableName}"))
                {
                    var dynamoDbClient = dynamoDbClientProvider.GetDynamoDbClient(connector, taskId);
                    var describeTableResponse = await dynamoDbClient.DescribeTableAsync(streamModel.TableName);
                    streamModel.StreamArn = describeTableResponse.Table.LatestStreamArn;
                    
                    if (string.IsNullOrEmpty(streamModel.StreamArn))
                    {
                        throw new InvalidOperationException(
                            $"DynamoDB Streams is not enabled on table '{streamModel.TableName}'. " +
                            "Please enable streams with StreamViewType set to NEW_AND_OLD_IMAGES.");
                    }
                    
                    logger.Debug("Retrieved stream ARN", new { StreamArn = streamModel.StreamArn, TableName = streamModel.TableName });
                }
            }

            // Get shard iterator if not already set
            if (string.IsNullOrEmpty(streamModel.ShardIterator))
            {
                using (logger.Track($"Getting shard iterator for stream: {streamModel.StreamArn}"))
                {
                    // First, describe the stream to get shard information
                    var describeStreamRequest = new DescribeStreamRequest
                    {
                        StreamArn = streamModel.StreamArn
                    };
                    
                    var describeResponse = await client.DescribeStreamAsync(describeStreamRequest);
                    
                    // Get the first active shard (in production, you'd want to handle multiple shards)
                    var shard = describeResponse.StreamDescription.Shards
                        .FirstOrDefault(s => s.SequenceNumberRange.EndingSequenceNumber == null);
                    
                    if (shard == null)
                    {
                        logger.Debug("No active shards found in stream");
                        return (new List<Record>(), null);
                    }

                    streamModel.ShardId = shard.ShardId;

                    // Get shard iterator
                    var getShardIteratorRequest = new GetShardIteratorRequest
                    {
                        StreamArn = streamModel.StreamArn,
                        ShardId = shard.ShardId,
                        ShardIteratorType = streamModel.ShardIteratorType
                    };

                    // Add sequence number if resuming
                    if (streamModel.ShardIteratorType == "AFTER_SEQUENCE_NUMBER" &&
                        !string.IsNullOrEmpty(streamModel.SequenceNumber))
                    {
                        getShardIteratorRequest.SequenceNumber = streamModel.SequenceNumber;
                    }

                    var shardIteratorResponse = await client.GetShardIteratorAsync(getShardIteratorRequest);
                    streamModel.ShardIterator = shardIteratorResponse.ShardIterator;
                    
                    logger.Debug("Got shard iterator", new { ShardId = shard.ShardId, ShardIteratorType = streamModel.ShardIteratorType });
                }
            }

            // Read records from the stream
            using (logger.Track("Getting records from stream"))
            {
                streamModel.Request.ShardIterator = streamModel.ShardIterator;
                var response = await client.GetRecordsAsync(streamModel.Request);

                logger.Debug("Stream records retrieved", new { Count = response.Records.Count });
                
                return (response.Records, response.NextShardIterator);
            }
        }
    }

}
