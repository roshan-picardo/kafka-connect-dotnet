using System.Text.Json;
using System.Text.Json.Nodes;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using IntegrationTests.Kafka.Connect.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace IntegrationTests.Kafka.Connect;

[Collection("Integration Tests")]
public class DynamoDbTestRunner(TestFixture fixture, ITestOutputHelper output) : BaseTestRunner(fixture, output)
{
    private readonly TestFixture _fixture = fixture;
    private IAmazonDynamoDB? _dynamoDbClient;
    private const string Target = "DynamoDb";

    private IAmazonDynamoDB GetDynamoDbClient()
    {
        if (_dynamoDbClient != null) return _dynamoDbClient;
        
        var serviceUrl = _fixture.Configuration.GetServiceEndpoint(Target);
        var config = new AmazonDynamoDBConfig
        {
            ServiceURL = serviceUrl
        };
        _dynamoDbClient = new AmazonDynamoDBClient(config);
        return _dynamoDbClient;
    }

    [Theory, TestPriority(2)]
    [MemberData(nameof(TestCases), Target)]
    public async Task Execute(TestCase testCase) => await Run(testCase, Target);

    protected override async Task Insert(Dictionary<string, string> properties, TestCaseRecord record)
    {
        var client = GetDynamoDbClient();
        var tableName = properties["tableName"];
        var item = ConvertJsonToAttributeValues(record.Value?.ToJsonString() ?? "{}");
        
        await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = item
        });
    }

    protected override async Task Update(Dictionary<string, string> properties, TestCaseRecord record)
    {
        var client = GetDynamoDbClient();
        var tableName = properties["tableName"];
        var item = ConvertJsonToAttributeValues(record.Value?.ToJsonString() ?? "{}");
        
        await client.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = item
        });
    }

    protected override async Task Delete(Dictionary<string, string> properties, TestCaseRecord record)
    {
        var client = GetDynamoDbClient();
        var tableName = properties["tableName"];
        var key = ConvertJsonToAttributeValues(record.Key?.ToJsonString() ?? "{}");
        
        await client.DeleteItemAsync(new DeleteItemRequest
        {
            TableName = tableName,
            Key = key
        });
    }

    protected override async Task Setup(Dictionary<string, string> properties)
    {
        // Tables are created by TestFixture during initialization
        // This method can be used for test-specific setup if needed
        await Task.CompletedTask;
    }

    protected override async Task Cleanup(Dictionary<string, string> properties)
    {
        // Optionally clean up test data
        // For now, we'll leave tables intact for debugging
        await Task.CompletedTask;
    }

    protected override async Task<JsonNode?> Search(Dictionary<string, string> properties, TestCaseRecord record)
    {
        var client = GetDynamoDbClient();
        var tableName = properties["tableName"];
        var key = ConvertJsonToAttributeValues(record.Key?.ToJsonString() ?? "{}");
        
        var response = await client.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = key
        });

        if (response.Item == null || response.Item.Count == 0)
        {
            return null;
        }

        return ConvertAttributeValuesToJson(response.Item);
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

    private static JsonNode ConvertAttributeValuesToJson(Dictionary<string, AttributeValue> attributes)
    {
        var jsonObject = new JsonObject();
        
        foreach (var kvp in attributes)
        {
            jsonObject[kvp.Key] = ConvertAttributeValueToJsonNode(kvp.Value);
        }
        
        return jsonObject;
    }

    private static JsonNode? ConvertAttributeValueToJsonNode(AttributeValue attributeValue)
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
