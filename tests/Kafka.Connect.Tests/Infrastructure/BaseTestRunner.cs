using System.Diagnostics;
using System.Text.Json.Nodes;
using Confluent.Kafka;
using Xunit;
using Xunit.Abstractions;

namespace IntegrationTests.Kafka.Connect.Infrastructure;

public abstract class BaseTestRunner(TestFixture fixture, ITestOutputHelper output) : IDisposable
{
    protected async Task Run(TestCase testCase, string target)
    {
        output.WriteLine($"Executing test: {testCase.Title}");
        if (testCase.Properties is { } properties)
        {
            try
            {
                properties.TryAdd("target", target);
                if(properties.GetValueOrDefault("skip")  != "setup")
                {
                    await Setup(properties);
                }
                foreach (var record in testCase.Records)
                {
                    await Task.Delay(record.Delay);
                    switch (record.Operation?.ToLowerInvariant())
                    {
                        case "search":
                            var searched = await Search(properties, record);
                            Validate(record.Value, searched);
                            break;
                        case "insert":
                            await Insert(properties, record);
                            break;
                        case "update":
                            await Update(properties, record);
                            break;
                        case "delete":
                            await Delete(properties, record);
                            break;
                        case "publish":
                            await Publish(properties, record);
                            break;
                        case "consume":
                            var consumed = await Consume(properties, record);
                            Validate(record.Value, consumed);
                            break;
                        default:
                            throw new InvalidOperationException($"Unknown operation: {record.Operation}");
                    }
                }
            }
            finally
            {
                if(properties.GetValueOrDefault("skip")  != "cleanup")
                {
                    await Cleanup(properties);
                }
            }
        }
    }
    
    protected abstract Task Setup(Dictionary<string, string> properties);
    protected abstract Task Cleanup(Dictionary<string, string> properties);
    protected abstract Task<JsonNode?> Search(Dictionary<string, string> properties, TestCaseRecord record);
    protected abstract Task Insert(Dictionary<string, string> properties, TestCaseRecord record);
    protected abstract Task Update(Dictionary<string, string> properties, TestCaseRecord record);
    protected abstract Task Delete(Dictionary<string, string> properties, TestCaseRecord record);
    private async Task<JsonNode?> Consume(Dictionary<string, string> properties, TestCaseRecord record)
    {
        var consumed = await ConsumeMessageAsync(properties["topic"]);
        return JsonNode.Parse(consumed.Message.Value);
    }

    private static void Validate(JsonNode? expected, JsonNode? actual)
    {
        if (expected == null)
        {
            Assert.Null(actual);
            return;
        }
        Validate(expected, actual, "");
    }


    private static void Validate(JsonNode? expected, JsonNode? actual, string path)
    {
        Assert.NotNull(actual);
    
        if (expected is JsonObject expectedObj && actual is JsonObject actualObj)
        {
            foreach (var expectedProperty in expectedObj)
            {
                var propertyPath = string.IsNullOrEmpty(path) ? expectedProperty.Key : $"{path}.{expectedProperty.Key}";
            
                Assert.True(actualObj.ContainsKey(expectedProperty.Key), 
                    $"Property '{expectedProperty.Key}' not found at path '{propertyPath}'");
            
                Validate(expectedProperty.Value, actualObj[expectedProperty.Key], propertyPath);
            }
        }
        else if (expected is JsonArray expectedArray && actual is JsonArray actualArray)
        {
            Assert.True(actualArray.Count >= expectedArray.Count, 
                $"Expected array to have at least {expectedArray.Count} items but found {actualArray.Count} at path '{path}'");
        
            for (int i = 0; i < expectedArray.Count; i++)
            {
                Validate(expectedArray[i], actualArray[i], $"{path}[{i}]");
            }
        }
        else
        {
            Assert.Equal(expected?.ToString(), actual.ToString());
        }
    }

    private async Task Publish(Dictionary<string, string> properties, TestCaseRecord record)
    {
        var result = await ProduceMessageAsync(properties["topic"], record.Key?.ToJsonString() ?? "",
            record.Value?.ToJsonString() ?? "");
        output.WriteLine($"Sent message to {result.Topic}:{result.Partition}:{result.Offset}");
    }

    private async Task<DeliveryResult<string, string>> ProduceMessageAsync(string topic, string key, string value)
    {
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = fixture.Configuration.GetServiceEndpoint("Kafka"),
            ClientId = "test-producer",
            SecurityProtocol = SecurityProtocol.Plaintext,
            MessageTimeoutMs = 30000,
            RequestTimeoutMs = 10000,
            DeliveryReportFields = "all",
            Acks = Acks.All,
            EnableIdempotence = true
        };

        using var producer = new ProducerBuilder<string, string>(producerConfig)
            .SetLogHandler((_, logMessage) =>
            {
                if (fixture.Configuration.DetailedLog)
                {
                    fixture.LogMessage(logMessage.Message);
                }
            })
            .SetErrorHandler((_, error) =>
            {
                if (fixture.Configuration.DetailedLog)
                {
                    fixture.LogMessage($"Kafka Producer Error: {error.Reason}");
                }
            })
            .Build();
            
        var message = new Message<string, string>
        {
            Key = key,
            Value = value
        };

        var result = await producer.ProduceAsync(topic, message);
        return result;
    }
    
    public async Task<ConsumeResult<string, string>> ConsumeMessageAsync(string topic)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = fixture.Configuration.GetServiceEndpoint("Kafka"),
            GroupId = "test-group-id",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
            SecurityProtocol = SecurityProtocol.Plaintext,
            SessionTimeoutMs = 30000,
            MaxPollIntervalMs = 30000,
            FetchWaitMaxMs = 500
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig)
            .SetLogHandler((_, logMessage) =>
            {
                if (fixture.Configuration.DetailedLog)
                {
                    fixture.LogMessage(logMessage.Message);
                }
            })
            .SetErrorHandler((_, error) =>
            {
                if (fixture.Configuration.DetailedLog)
                {
                    fixture.LogMessage($"Kafka Consumer Error: {error.Reason}");
                }
            })
            .Build();
        consumer.Subscribe(topic);

        return await Task.Run(() =>
        {
            try
            {
                var result = consumer.Consume();
                return result;
            }
            finally
            {
                consumer.Unsubscribe();
            }
        });
    }


    public virtual void Dispose()
    {
        // Default implementation - can be overridden by derived classes
    }
    
    public static IEnumerable<object[]> TestCases(string testcase) => TestCaseProvider.GetTestCases(testcase);
}

public record SchemaRecord(JsonNode? Key, JsonNode Value);
public record TestCaseConfig(string Schema, string? Folder, string[]? Files, string? Target = null, bool Skip = false);

public record TestCaseRecord(string Operation, int Delay, JsonNode? Key, JsonNode? Value);

public record TestCase(string Title, Dictionary<string, string> Properties, TestCaseRecord[] Records, bool Skip = false)
{
    public override string ToString()
    {
        return $"Title: {Title}, Topic: {Properties["topic"]}, Records: {Records.Length}";
    }
}