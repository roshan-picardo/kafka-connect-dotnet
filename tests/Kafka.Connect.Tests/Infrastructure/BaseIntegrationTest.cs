using System.Text.Json.Nodes;
using IntegrationTests.Kafka.Connect.Infrastructure;
using Xunit.Abstractions;

namespace IntegrationTests.Kafka.Connect.Infrastructure;

public abstract class BaseIntegrationTest<TTestCase>(TestFixture fixture, ITestOutputHelper output) : IDisposable
{
    protected async Task ExecuteTestAsync(TTestCase testCase)
    {
        var testTitle = GetTestTitle(testCase);
        output.WriteLine($"Executing test: {testTitle}");

        try
        {
            await SetupAsync(testCase);

            await PublishAsync(testCase);

            await Task.Delay(5000);

            await ValidateAsync(testCase);

            output.WriteLine($"Test '{testTitle}' completed successfully");
        }
        finally
        {
            await CleanupAsync(testCase);
        }
    }

    protected abstract Task SetupAsync(TTestCase testCase);

    private async Task PublishAsync(TTestCase testCase)
    {
        var topicName = GetTopicName(testCase);
        var records = GetRecords(testCase);

        await fixture.CreateTopicAsync(topicName);

        await SendMessagesToKafka(topicName, records);
    }

    protected abstract Task ValidateAsync(TTestCase testCase);

    protected abstract Task CleanupAsync(TTestCase testCase);

    protected abstract string GetTestTitle(TTestCase testCase);

    protected abstract string GetTopicName(TTestCase testCase);

    protected abstract IEnumerable<(string Key, string Value)> GetRecords(TTestCase testCase);

    private async Task SendMessagesToKafka(string topicName, IEnumerable<(string Key, string Value)> records)
    {
        foreach (var (key, value) in records)
        {
            var result = await fixture.ProduceMessageAsync(topicName, key, value);
            output.WriteLine($"Sent message to {result.Topic}:{result.Partition}:{result.Offset}");
        }
    }

    public virtual void Dispose()
    {
        // Default implementation - can be overridden by derived classes
    }
}