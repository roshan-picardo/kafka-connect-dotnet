using System.Data;
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
        // Ensure all connectors are healthy before running the test
        await fixture.EnsureConnectorsHealthyAsync();
        
        output.WriteLine($"Executing test: {testCase.Title}");
        if (testCase.Properties is { } properties)
        {
            properties.TryAdd("target", target);
            foreach (var record in testCase.Records)
            {
                await Task.Delay(record.Delay);
                switch (record.Operation?.ToLowerInvariant())
                {
                    case "setup":
                        output.WriteLine($"[SETUP] Starting setup with script: {record.Script}");
                        properties["setup"] = record.Script;
                        await Setup(properties);
                        output.WriteLine($"[SETUP] Completed successfully");
                        break;
                    case "search":
                        output.WriteLine($"[SEARCH] Starting search operation");
                        // Retry search and validation up to 30 times with 1 second delay (30 seconds total)
                        Exception? lastException = null;
                        for (var attempt = 1; attempt <= 30; attempt++)
                        {
                            try
                            {
                                if (attempt > 1)
                                {
                                    output.WriteLine($"[SEARCH] Retry attempt {attempt}/30");
                                }
                                var searched = await Search(properties, record);
                                output.WriteLine($"[SEARCH] Search returned: {searched?.ToJsonString() ?? "null"}");
                                Validate(record.Value, searched);
                                output.WriteLine($"[SEARCH] Validation successful on attempt {attempt}");
                                lastException = null; // Clear exception on success
                                break; // Success - exit retry loop
                            }
                            catch (Exception ex)
                            {
                                lastException = ex;
                                output.WriteLine($"[SEARCH] Attempt {attempt}/30 failed: {ex.Message}");
                                // If not last attempt, wait before retrying
                                if (attempt < 30)
                                {
                                    await Task.Delay(1000);
                                }
                            }
                        }
                        // If we exhausted all retries, throw the last exception
                        if (lastException != null)
                        {
                            output.WriteLine($"[SEARCH] All 30 attempts failed. Last error: {lastException.Message}");
                            throw lastException;
                        }
                        break;
                    case "insert":
                        output.WriteLine($"[INSERT] Starting insert operation with key: {record.Key?.ToJsonString() ?? "null"}");
                        await Insert(properties, record);
                        output.WriteLine($"[INSERT] Completed successfully");
                        break;
                    case "update":
                        output.WriteLine($"[UPDATE] Starting update operation with key: {record.Key?.ToJsonString() ?? "null"}");
                        await Update(properties, record);
                        output.WriteLine($"[UPDATE] Completed successfully");
                        break;
                    case "delete":
                        output.WriteLine($"[DELETE] Starting delete operation with key: {record.Key?.ToJsonString() ?? "null"}");
                        await Delete(properties, record);
                        output.WriteLine($"[DELETE] Completed successfully");
                        break;
                    case "publish":
                        output.WriteLine($"[PUBLISH] Starting publish to topic: {properties.GetValueOrDefault("topic", "unknown")}");
                        output.WriteLine($"[PUBLISH] Key: {record.Key?.ToJsonString() ?? "null"}");
                        output.WriteLine($"[PUBLISH] Value: {record.Value?.ToJsonString() ?? "null"}");
                        await Publish(properties, record);
                        output.WriteLine($"[PUBLISH] Completed successfully");
                        break;
                    case "consume":
                        output.WriteLine($"[CONSUME] Starting consume from topic: {properties.GetValueOrDefault("topic", "unknown")}");
                        // Retry consume and validation up to 30 times with 1 second delay (30 seconds total)
                        Exception? lastConsumeException = null;
                        for (var attempt = 1; attempt <= 30; attempt++)
                        {
                            try
                            {
                                if (attempt > 1)
                                {
                                    output.WriteLine($"[CONSUME] Retry attempt {attempt}/30");
                                }
                                var consumed = await Consume(properties, record);
                                output.WriteLine($"[CONSUME] Consumed message: {consumed?.ToJsonString() ?? "null"}");
                                Validate(record.Value, consumed);
                                output.WriteLine($"[CONSUME] Validation successful on attempt {attempt}");
                                lastConsumeException = null; // Clear exception on success
                                break; // Success - exit retry loop
                            }
                            catch (Exception ex)
                            {
                                lastConsumeException = ex;
                                output.WriteLine($"[CONSUME] Attempt {attempt}/30 failed: {ex.Message}");
                                // If not last attempt, wait before retrying
                                if (attempt < 30)
                                {
                                    await Task.Delay(1000);
                                }
                            }
                        }
                        // If we exhausted all retries, throw the last exception
                        if (lastConsumeException != null)
                        {
                            output.WriteLine($"[CONSUME] All 30 attempts failed. Last error: {lastConsumeException.Message}");
                            throw lastConsumeException;
                        }
                        break;
                    case "cleanup":
                        output.WriteLine($"[CLEANUP] Starting cleanup with script: {record.Script}");
                        properties["cleanup"] = record.Script;
                        await Cleanup(properties);
                        output.WriteLine($"[CLEANUP] Completed successfully");
                        break;
                    default:
                        throw new InvalidOperationException($"Unknown operation: {record.Operation}");
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
        return consumed == null ? null : JsonNode.Parse(consumed.Message.Value);
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
            if (expected?.ToString() == "*")
            {
                return; 
            }
            Assert.Equal(expected?.ToString(), actual.ToString());
        }
    }

    private async Task Publish(Dictionary<string, string> properties, TestCaseRecord record)
    {
        var result = await ProduceMessageAsync(properties["topic"], record.Key?.ToJsonString() ?? "",
            record.Value?.ToJsonString() ?? "");
        output.WriteLine($"[PUBLISH] Message delivered to {result.Topic}:{result.Partition}:{result.Offset}");
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

    private async Task<ConsumeResult<string, string>?> ConsumeMessageAsync(string topic)
    {
        output.WriteLine($"[CONSUME] Creating consumer for topic: {topic}");
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = fixture.Configuration.GetServiceEndpoint("Kafka"),
            GroupId = "test-group-id",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            SecurityProtocol = SecurityProtocol.Plaintext,
            SessionTimeoutMs = 6000,
            MaxPollIntervalMs = 10000,
            FetchWaitMaxMs = 100
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
        output.WriteLine($"[CONSUME] Subscribed to topic: {topic}");

        return await Task.Run(() =>
        {
            try
            {
                var loop = 300; // Wait up to 30 seconds for initial message
                ConsumeResult<string, string>? lastResult = null;
                ConsumeResult<string, string>? result;
                
                output.WriteLine($"[CONSUME] Polling for messages (max 30 seconds)...");
                do
                {
                    result = consumer.Consume(100);
                    if (result != null)
                    {
                        lastResult = result;
                        output.WriteLine($"[CONSUME] Received message from {result.Topic}:{result.Partition}:{result.Offset}");
                    }
                } while (result == null && loop-- > 0);

                if (lastResult == null)
                {
                    output.WriteLine($"[CONSUME] No messages received after 30 seconds");
                    throw new DataException("Consumer returned null after waiting for 30 seconds...");
                }

                output.WriteLine($"[CONSUME] Checking for additional messages...");
                var additionalCount = 0;
                while (true)
                {
                    result = consumer.Consume(100);
                    if (result == null)
                    {
                        break;
                    }
                    additionalCount++;
                    lastResult = result;
                    output.WriteLine($"[CONSUME] Received additional message #{additionalCount} from {result.Topic}:{result.Partition}:{result.Offset}");
                }

                output.WriteLine($"[CONSUME] Committing offset for {lastResult.Topic}:{lastResult.Partition}:{lastResult.Offset}");
                consumer.Commit(lastResult);

                return lastResult;
            }
            finally
            {
                output.WriteLine($"[CONSUME] Unsubscribing from topic: {topic}");
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
public record TestCaseConfig(string Schema, string? Folder, string[]? Files, string? Target = null, bool Skip = false, TestCaseInitScripts? Setup = null);

public record TestCaseRecord(string Operation, int Delay, string Script, JsonNode? Key, JsonNode? Value);

public record TestCase(string Title, Dictionary<string, string> Properties, TestCaseRecord[] Records, bool Skip = false)
{
    public override string ToString()
    {
        return $"title: {Title}, topic: {Properties["topic"]}, records: {Records.Length}, at: {DateTime.Now.ToString("HH:mm:ss")}";
    }
}

public record TestCaseInitScripts(string Database, string[] Scripts);