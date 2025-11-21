using System.Text.Json.Nodes;
using Xunit.Abstractions;

namespace IntegrationTests.Kafka.Connect.Infrastructure;

public abstract class BaseTests<T>(TestFixture fixture, ITestOutputHelper output) : IDisposable
    where T : TargetProperties
{
    protected async Task ExecuteTest(TestCase<T> testCase)
    {
        output.WriteLine($"Executing test: {testCase.Title}");
        if (testCase.Properties is { } properties)
        {
            try
            {
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
                            await Publish(testCase.Topic, record);
                            break;
                        case "consume":
                            await Consume(testCase.Topic, record);
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
    
    protected abstract Task Setup(T properties);
    protected abstract Task Cleanup(T properties);
    protected abstract Task Search(T properties, TestCaseRecord record);
    protected abstract Task Insert(T properties, TestCaseRecord record);
    protected abstract Task Update(T properties, TestCaseRecord record);
    protected abstract Task Delete(T properties, TestCaseRecord record);

    private async Task Publish(string topic, TestCaseRecord record)
    {
        var result = await fixture.ProduceMessageAsync(topic, record.Key?.ToJsonString() ?? "",
            record.Value?.ToJsonString() ?? "");
        output.WriteLine($"Sent message to {result.Topic}:{result.Partition}:{result.Offset}");
    }

    private async Task Consume(string topic, TestCaseRecord record)
    {
        await Task.Delay(1);
    }

    public virtual void Dispose()
    {
        // Default implementation - can be overridden by derived classes
    }
    
    public static IEnumerable<object[]> TestCases(string testcase) => TestCaseProvider.GetTestCases<T>(testcase);
}

public record SchemaRecord(JsonNode? Key, JsonNode Value);
public record TestCaseConfig(string Schema, string? Folder, string[]? Files, string? Target = null);
public record TargetProperties;
public record TestCaseRecord(string Operation, int Delay, JsonNode? Key, JsonNode? Value);

public record TestCase<T>(string Title, string Topic, T Properties, TestCaseRecord[] Records)
{
    public override string ToString()
    {
        return $"Title: {Title}, Topic: {Topic}, {Properties?.ToString()}, Records: {Records?.Length}";
    }
};

