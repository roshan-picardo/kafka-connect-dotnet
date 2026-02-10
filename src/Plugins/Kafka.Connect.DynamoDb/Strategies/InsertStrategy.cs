using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Strategies;

namespace Kafka.Connect.DynamoDb.Strategies;

public class InsertStrategy(ILogger<InsertStrategy> logger) : Strategy<WriteRequest>
{
    protected override Task<StrategyModel<WriteRequest>> BuildModels(string connector, ConnectRecord record)
    {
        using (logger.Track("Creating insert write request"))
        {
            var item = ConvertJsonToAttributeValues(record.Deserialized.Value.ToJsonString());
            
            return Task.FromResult(new StrategyModel<WriteRequest>
            {
                Key = record.Key,
                Status = Status.Inserting,
                Model = new WriteRequest
                {
                    PutRequest = new PutRequest
                    {
                        Item = item
                    }
                }
            });
        }
    }

    protected override Task<StrategyModel<WriteRequest>> BuildModels(string connector, CommandRecord record)
    {
        throw new NotImplementedException();
    }

    private static Dictionary<string, AttributeValue> ConvertJsonToAttributeValues(string json)
    {
        var document = JsonDocument.Parse(json);
        return ConvertJsonElementToAttributeValues(document.RootElement);
    }

    private static Dictionary<string, AttributeValue> ConvertJsonElementToAttributeValues(JsonElement element)
    {
        var attributes = new Dictionary<string, AttributeValue>();

        foreach (var property in element.EnumerateObject())
        {
            attributes[property.Name] = ConvertToAttributeValue(property.Value);
        }

        return attributes;
    }

    private static AttributeValue ConvertToAttributeValue(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => new AttributeValue { S = element.GetString() },
            JsonValueKind.Number => new AttributeValue { N = element.GetRawText() },
            JsonValueKind.True => new AttributeValue { BOOL = true },
            JsonValueKind.False => new AttributeValue { BOOL = false },
            JsonValueKind.Null => new AttributeValue { NULL = true },
            JsonValueKind.Array => new AttributeValue
            {
                L = element.EnumerateArray().Select(ConvertToAttributeValue).ToList()
            },
            JsonValueKind.Object => new AttributeValue
            {
                M = ConvertJsonElementToAttributeValues(element)
            },
            _ => new AttributeValue { NULL = true }
        };
    }
}
