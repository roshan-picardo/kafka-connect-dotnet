using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using DotNet.Testcontainers.Networks;
using System.Text.Json;

namespace IntegrationTests.Kafka.Connect.Infrastructure.Fixtures;

public class DynamoDbFixture : DatabaseFixture
{
    public DynamoDbFixture(
        TestConfiguration configuration,
        Action<string, string> logMessage,
        IContainerService containerService,
        INetwork network,
        TestCaseConfig[]? testConfigs)
        : base(configuration, logMessage, containerService, network, testConfigs)
    {
    }

    protected override string GetTargetName() => "dynamodb";

    public override async Task WaitForReadyAsync()
    {
        var serviceUrl = Configuration.GetServiceEndpoint("DynamoDb");

        for (var attempt = 1; attempt <= DatabaseReadyMaxAttempts; attempt++)
        {
            AmazonDynamoDBClient? client = null;
            try
            {
                var config = new AmazonDynamoDBConfig
                {
                    ServiceURL = serviceUrl,
                    AuthenticationRegion = "us-east-1",
                    UseHttp = true,
                    MaxErrorRetry = 0,
                    Timeout = TimeSpan.FromSeconds(5)
                };

                var credentials = new BasicAWSCredentials("dummy", "dummy");
                client = new AmazonDynamoDBClient(credentials, config);

                await client.ListTablesAsync();

                LogMessage($"DynamoDB is ready (attempt {attempt})", "");
                return;
            }
            catch (Exception ex)
            {
                if (attempt == DatabaseReadyMaxAttempts)
                {
                    throw new TimeoutException(
                        $"DynamoDB did not become ready after {DatabaseReadyMaxAttempts} attempts", ex);
                }

                LogMessage($"DynamoDB not ready yet (attempt {attempt}/{DatabaseReadyMaxAttempts}): {ex.Message}", "");
                await Task.Delay(DatabaseReadyDelayMs);
            }
            finally
            {
                client?.Dispose();
            }
        }
    }

    public override async Task ExecuteScriptsAsync(string database, string[] scripts)
    {
        var serviceUrl = Configuration.GetServiceEndpoint("DynamoDb");
        var config = new AmazonDynamoDBConfig
        {
            ServiceURL = serviceUrl,
            AuthenticationRegion = "us-east-1",
            UseHttp = true
        };

        var credentials = new BasicAWSCredentials("dummy", "dummy");
        using var client = new AmazonDynamoDBClient(credentials, config);

        foreach (var script in scripts)
        {
            try
            {
                var tableConfig = JsonDocument.Parse(script);
                var root = tableConfig.RootElement;

                if (!root.TryGetProperty("TableName", out var tableNameElement))
                {
                    LogMessage("Missing 'TableName' property in DynamoDB script", "");
                    continue;
                }

                var tableName = tableNameElement.GetString();
                if (string.IsNullOrEmpty(tableName))
                {
                    LogMessage("Invalid table name in DynamoDB script", "");
                    continue;
                }

                try
                {
                    await client.DescribeTableAsync(tableName);
                    LogMessage($"DynamoDB table already exists: {tableName}", "");
                    continue;
                }
                catch (ResourceNotFoundException)
                {
                }

                var keySchema = new List<KeySchemaElement>();
                var attributeDefinitions = new List<AttributeDefinition>();

                if (root.TryGetProperty("KeySchema", out var keySchemaElement))
                {
                    foreach (var key in keySchemaElement.EnumerateArray())
                    {
                        if (key.TryGetProperty("AttributeName", out var attrName) &&
                            key.TryGetProperty("KeyType", out var keyType))
                        {
                            keySchema.Add(new KeySchemaElement
                            {
                                AttributeName = attrName.GetString(),
                                KeyType = keyType.GetString()?.ToUpperInvariant() == "RANGE"
                                    ? KeyType.RANGE
                                    : KeyType.HASH
                            });
                        }
                    }
                }

                if (root.TryGetProperty("AttributeDefinitions", out var attrDefsElement))
                {
                    foreach (var attr in attrDefsElement.EnumerateArray())
                    {
                        if (attr.TryGetProperty("AttributeName", out var attrName) &&
                            attr.TryGetProperty("AttributeType", out var attrType))
                        {
                            var typeString = attrType.GetString()?.ToUpperInvariant();
                            var scalarType = typeString switch
                            {
                                "N" => ScalarAttributeType.N,
                                "B" => ScalarAttributeType.B,
                                _ => ScalarAttributeType.S
                            };

                            attributeDefinitions.Add(new AttributeDefinition
                            {
                                AttributeName = attrName.GetString(),
                                AttributeType = scalarType
                            });
                        }
                    }
                }

                var request = new CreateTableRequest
                {
                    TableName = tableName,
                    KeySchema = keySchema,
                    AttributeDefinitions = attributeDefinitions,
                    BillingMode = BillingMode.PAY_PER_REQUEST
                };

                // Handle StreamSpecification if present
                if (root.TryGetProperty("StreamSpecification", out var streamSpecElement))
                {
                    var streamSpec = new StreamSpecification();

                    if (streamSpecElement.TryGetProperty("StreamEnabled", out var streamEnabled))
                    {
                        streamSpec.StreamEnabled = streamEnabled.GetBoolean();
                    }

                    if (streamSpecElement.TryGetProperty("StreamViewType", out var streamViewType))
                    {
                        var viewTypeString = streamViewType.GetString()?.ToUpperInvariant();
                        streamSpec.StreamViewType = viewTypeString switch
                        {
                            "KEYS_ONLY" => StreamViewType.KEYS_ONLY,
                            "NEW_IMAGE" => StreamViewType.NEW_IMAGE,
                            "OLD_IMAGE" => StreamViewType.OLD_IMAGE,
                            "NEW_AND_OLD_IMAGES" => StreamViewType.NEW_AND_OLD_IMAGES,
                            _ => StreamViewType.NEW_AND_OLD_IMAGES
                        };
                    }

                    request.StreamSpecification = streamSpec;
                }

                await client.CreateTableAsync(request);

                const int maxAttempts = 30;
                for (var i = 0; i < maxAttempts; i++)
                {
                    var describeResponse = await client.DescribeTableAsync(tableName);
                    if (describeResponse.Table.TableStatus == TableStatus.ACTIVE)
                    {
                        break;
                    }

                    await Task.Delay(1000);
                }
            }
            catch (Exception ex)
            {
                LogMessage($"Failed to execute DynamoDB script: {script} - {ex.Message}", "");
                throw;
            }
        }
    }
}
