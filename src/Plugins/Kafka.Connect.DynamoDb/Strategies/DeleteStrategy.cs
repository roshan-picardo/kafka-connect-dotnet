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
using Kafka.Connect.Plugin.Providers;
using Kafka.Connect.Plugin.Strategies;

namespace Kafka.Connect.DynamoDb.Strategies;

public class DeleteStrategy(ILogger<DeleteStrategy> logger, IConfigurationProvider configurationProvider)
    : Strategy<WriteRequest>
{
    protected override Task<StrategyModel<WriteRequest>> BuildModels(string connector, ConnectRecord record)
    {
        using (logger.Track("Creating delete write request"))
        {
            var config = configurationProvider.GetPluginConfig<PluginConfig>(connector);
            var valueDict = record.Deserialized.Value.ToDictionary();
            var key = BuildKeyFromFilter(config.Filter, valueDict);
            
            return Task.FromResult(new StrategyModel<WriteRequest>
            {
                Status = Status.Deleting,
                Model = new WriteRequest
                {
                    DeleteRequest = new DeleteRequest
                    {
                        Key = key
                    }
                }
            });
        }
    }

    protected override Task<StrategyModel<WriteRequest>> BuildModels(string connector, CommandRecord record) 
        => throw new NotImplementedException();

    private static Dictionary<string, AttributeValue> BuildKeyFromFilter(string filter, IDictionary<string, object> values)
    {
        var key = new Dictionary<string, AttributeValue>();
        
        if (string.IsNullOrEmpty(filter))
        {
            return key;
        }

        // Parse filter to extract key fields
        // Expected format: "id={id}" or "id={id},sort={sort}"
        var filterParts = filter.Split(',');
        foreach (var part in filterParts)
        {
            var keyValue = part.Split('=');
            if (keyValue.Length == 2)
            {
                var fieldName = keyValue[0].Trim();
                var placeholder = keyValue[1].Trim().Trim('{', '}');
                
                if (values.TryGetValue(placeholder, out var value))
                {
                    key[fieldName] = ConvertToAttributeValue(value);
                }
            }
        }

        return key;
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
