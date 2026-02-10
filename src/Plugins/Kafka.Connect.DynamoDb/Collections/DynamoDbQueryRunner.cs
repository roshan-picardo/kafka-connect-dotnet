using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
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

}
