using System.Diagnostics;
using System.Text.Json.Nodes;
using Confluent.Kafka;
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
                await Setup(properties);
                foreach (var record in testCase.Records)
                {
                    await Task.Delay(record.Delay);
                    switch (record.Operation?.ToLowerInvariant())
                    {
                        case "search":
                            await Search(properties, record);
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
                            await Consume(properties, record);
                            break;
                        default:
                            throw new InvalidOperationException($"Unknown operation: {record.Operation}");
                    }
                }
            }
            finally
            {
                await Cleanup(properties);
            }
        }
    }
    
    protected abstract Task Setup(Dictionary<string, string> properties);
    protected abstract Task Cleanup(Dictionary<string, string> properties);
    protected abstract Task Search(Dictionary<string, string> properties, TestCaseRecord record);
    protected abstract Task Insert(Dictionary<string, string> properties, TestCaseRecord record);
    protected abstract Task Update(Dictionary<string, string> properties, TestCaseRecord record);
    protected abstract Task Delete(Dictionary<string, string> properties, TestCaseRecord record);

    private async Task Publish(Dictionary<string, string> properties, TestCaseRecord record)
    {
        var result = await ProduceMessageAsync(properties["topic"], record.Key?.ToJsonString() ?? "",
            record.Value?.ToJsonString() ?? "");
        output.WriteLine($"Sent message to {result.Topic}:{result.Partition}:{result.Offset}");
    }
    
    protected async Task<DeliveryResult<string, string>> ProduceMessageAsync(string topic, string key, string value)
    {
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = fixture.Configuration.Shakedown.Kafka,
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
            BootstrapServers = fixture.Configuration.Shakedown.Kafka,
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

    private async Task Consume(Dictionary<string, string> properties, TestCaseRecord record)
    {
        var consumed = await ConsumeMessageAsync(properties["topic"]);
        await Task.Delay(1);
    }

    public virtual void Dispose()
    {
        // Default implementation - can be overridden by derived classes
    }
    
    public static IEnumerable<object[]> TestCases(string testcase) => TestCaseProvider.GetTestCases(testcase);
}

public record SchemaRecord(JsonNode? Key, JsonNode Value);
public record TestCaseConfig(string Schema, string? Folder, string[]? Files, string? Target = null);
public record TargetProperties;
public record TestCaseRecord(string Operation, int Delay, JsonNode? Key, JsonNode? Value);

public record TestCase(string Title, Dictionary<string, string> Properties, TestCaseRecord[] Records)
{
    public override string ToString()
    {
        return $"Title: {Title}, Topic: {Properties["topic"]}, Records: {Records.Length}";
    }
}