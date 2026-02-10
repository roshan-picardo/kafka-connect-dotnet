using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using Kafka.Connect.DynamoDb.Models;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;

namespace Kafka.Connect.DynamoDb.Strategies;

public class ReadStrategy(ILogger<ReadStrategy> logger) : Strategy<ScanModel>
{
    protected override Task<StrategyModel<ScanModel>> BuildModels(string connector, ConnectRecord record)
    {
        throw new NotImplementedException();
    }

    protected override Task<StrategyModel<ScanModel>> BuildModels(string connector, CommandRecord record)
    {
        using (logger.Track("Creating read models"))
        {
            var command = record.GetCommand<CommandConfig>();
            var model = new StrategyModel<ScanModel> { Status = Status.Selecting };
            
            if (!record.IsChangeLog())
            {
                var scanRequest = new ScanRequest
                {
                    TableName = command.TableName,
                    Limit = record.BatchSize
                };

                // Add filter expression if filters are provided
                if (command.Filters != null && command.Filters.Any())
                {
                    var filterExpressions = new List<string>();
                    var expressionAttributeNames = new Dictionary<string, string>();
                    var expressionAttributeValues = new Dictionary<string, AttributeValue>();
                    
                    var filterIndex = 0;
                    foreach (var filter in command.Filters)
                    {
                        var attributeName = $"#attr{filterIndex}";
                        var attributeValue = $":val{filterIndex}";
                        
                        expressionAttributeNames[attributeName] = filter.Key;
                        expressionAttributeValues[attributeValue] = ConvertToAttributeValue(
                            filter.Value is JsonElement je ? je.GetValue() : filter.Value);
                        
                        filterExpressions.Add($"{attributeName} > {attributeValue}");
                        filterIndex++;
                    }

                    if (filterExpressions.Any())
                    {
                        scanRequest.FilterExpression = string.Join(" AND ", filterExpressions);
                        scanRequest.ExpressionAttributeNames = expressionAttributeNames;
                        scanRequest.ExpressionAttributeValues = expressionAttributeValues;
                    }
                }

                model.Model = new ScanModel
                {
                    Operation = "SCAN",
                    Request = scanRequest
                };
            }

            return Task.FromResult(model);
        }
    }

    private static AttributeValue ConvertToAttributeValue(object value)
    {
        return value switch
        {
            string s => new AttributeValue { S = s },
            int i => new AttributeValue { N = i.ToString() },
            long l => new AttributeValue { N = l.ToString() },
            double d => new AttributeValue { N = d.ToString() },
            float f => new AttributeValue { N = f.ToString() },
            bool b => new AttributeValue { BOOL = b },
            null => new AttributeValue { NULL = true },
            _ => new AttributeValue { S = value.ToString() }
        };
    }
}
