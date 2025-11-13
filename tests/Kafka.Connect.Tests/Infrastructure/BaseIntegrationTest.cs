using Xunit.Abstractions;

namespace IntegrationTests.Kafka.Connect.Infrastructure;

public abstract class BaseIntegrationTest<T>(TestFixture fixture, ITestOutputHelper output) : IDisposable where T : BaseSinkRecord
{
    protected async Task ExecuteTestAsync(TestCase testCase)
    {
        var testTitle = GetTestTitle(testCase);
        output.WriteLine($"Executing test: {testTitle}");

        try
        {
            await SetupAsync(testCase.Sink.Properties as T);

            await PublishAsync(testCase);

            await Task.Delay(5000);

            await ValidateAsync(testCase.Sink.Properties as T);

            output.WriteLine($"Test '{testTitle}' completed successfully");
        }
        finally
        {
            await CleanupAsync(testCase.Sink.Properties as T);
        }
    }

    protected abstract Task SetupAsync(T? sink);

    private async Task PublishAsync(TestCase testCase)
    {
        var topicName = testCase.Sink.Topic;
        await fixture.CreateTopicAsync(topicName);
        await SendMessagesToKafka(topicName, GetRecords(testCase));
    }

    protected abstract Task ValidateAsync(T? sink);

    protected abstract Task CleanupAsync(T? sink);

    private string GetTestTitle(TestCase testCase) => testCase.Title;

    private IEnumerable<(string Key, string Value)> GetRecords(TestCase testCase) =>
        testCase.Records.Select(record => (
            Key: record.Key?.ToString() ?? "",
            Value: record.Value.ToJsonString()
        ));

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