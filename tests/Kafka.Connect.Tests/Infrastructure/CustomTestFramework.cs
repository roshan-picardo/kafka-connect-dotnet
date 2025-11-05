using System.Reflection;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace IntegrationTests.Kafka.Connect.Infrastructure;

public class CustomTestFramework : XunitTestFramework
{
    public CustomTestFramework(IMessageSink messageSink) : base(messageSink)
    {
    }

    protected override ITestFrameworkExecutor CreateExecutor(AssemblyName assemblyName)
    {
        return new CustomTestFrameworkExecutor(assemblyName, SourceInformationProvider, DiagnosticMessageSink);
    }
}

public class CustomTestFrameworkExecutor : XunitTestFrameworkExecutor
{
    public CustomTestFrameworkExecutor(AssemblyName assemblyName, ISourceInformationProvider sourceInformationProvider, IMessageSink diagnosticMessageSink)
        : base(assemblyName, sourceInformationProvider, diagnosticMessageSink)
    {
    }

    protected override async void RunTestCases(IEnumerable<IXunitTestCase> testCases, IMessageSink executionMessageSink, ITestFrameworkExecutionOptions executionOptions)
    {
        var interceptingSink = new TestResultInterceptingSink(executionMessageSink);
        base.RunTestCases(testCases, interceptingSink, executionOptions);
        
        // Display summary after all tests complete
        await Task.Delay(100); // Small delay to ensure all results are processed
        TestResultCollector.DisplaySummary();
    }
}

public class TestResultInterceptingSink : MarshalByRefObject, IMessageSink
{
    private readonly IMessageSink _innerSink;
    private readonly Dictionary<string, DateTime> _testStartTimes = new();

    public TestResultInterceptingSink(IMessageSink innerSink)
    {
        _innerSink = innerSink;
    }

    public bool OnMessage(IMessageSinkMessage message)
    {
        // Forward the message to the original sink first
        var result = _innerSink.OnMessage(message);

        // Intercept test results
        switch (message)
        {
            case ITestStarting testStarting:
                _testStartTimes[testStarting.Test.DisplayName] = DateTime.UtcNow;
                break;

            case ITestPassed testPassed:
                var passedDuration = CalculateDuration(testPassed.Test.DisplayName);
                TestResultCollector.AddResult(new TestResult
                {
                    TestName = testPassed.Test.DisplayName,
                    Status = TestStatus.Passed,
                    Duration = passedDuration
                });
                break;

            case ITestFailed testFailed:
                var failedDuration = CalculateDuration(testFailed.Test.DisplayName);
                TestResultCollector.AddResult(new TestResult
                {
                    TestName = testFailed.Test.DisplayName,
                    Status = TestStatus.Failed,
                    Duration = failedDuration,
                    ErrorMessage = testFailed.Messages?.FirstOrDefault()
                });
                break;

            case ITestSkipped testSkipped:
                TestResultCollector.AddResult(new TestResult
                {
                    TestName = testSkipped.Test.DisplayName,
                    Status = TestStatus.Skipped,
                    SkipReason = testSkipped.Reason
                });
                break;
        }

        return result;
    }

    private double CalculateDuration(string testName)
    {
        if (_testStartTimes.TryGetValue(testName, out var startTime))
        {
            var duration = DateTime.UtcNow - startTime;
            _testStartTimes.Remove(testName);
            return duration.TotalSeconds;
        }
        return 0;
    }
}