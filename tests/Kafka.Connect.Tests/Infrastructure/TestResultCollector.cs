using System.Collections.Concurrent;
using System.Text.RegularExpressions;
using Xunit.Abstractions;

namespace IntegrationTests.Kafka.Connect.Infrastructure;

public class TestResultCollector
{
    private static readonly ConcurrentQueue<TestResult> TestResults = new();
    private static readonly object Lock = new();
    private static bool _summaryDisplayed = false;
    private static readonly Regex TestResultPattern = new(@"^\s*(Passed|Failed|Skipped)\s+(.+?)\s+\[(\d+(?:\.\d+)?)\s*s\]", RegexOptions.Compiled);

    public static void AddResult(TestResult result)
    {
        TestResults.Enqueue(result);
    }

    public static void ParseAndAddResult(string logLine)
    {
        var match = TestResultPattern.Match(logLine);
        if (match.Success)
        {
            var status = match.Groups[1].Value;
            var testName = match.Groups[2].Value.Trim();
            var duration = double.Parse(match.Groups[3].Value);

            var testStatus = status switch
            {
                "Passed" => TestStatus.Passed,
                "Failed" => TestStatus.Failed,
                "Skipped" => TestStatus.Skipped,
                _ => TestStatus.Passed
            };

            AddResult(new TestResult
            {
                TestName = testName,
                Status = testStatus,
                Duration = duration
            });
        }
    }

    public static void DisplaySummary()
    {
        lock (Lock)
        {
            if (_summaryDisplayed)
                return;

            _summaryDisplayed = true;
        }

        var results = new List<TestResult>();
        while (TestResults.TryDequeue(out var result))
        {
            results.Add(result);
        }

        if (results.Count == 0)
            return;

        var passed = results.Where(r => r.Status == TestStatus.Passed).ToList();
        var failed = results.Where(r => r.Status == TestStatus.Failed).ToList();
        var skipped = results.Where(r => r.Status == TestStatus.Skipped).ToList();

        Console.WriteLine();
        Console.WriteLine("========== TEST RESULTS SUMMARY ==========");
        Console.WriteLine();

        // Display passed tests
        if (passed.Count > 0)
        {
            Console.WriteLine($"✅ PASSED TESTS ({passed.Count}):");
            foreach (var test in passed.OrderBy(t => t.TestName))
            {
                Console.WriteLine($"  Passed {test.TestName} [{test.Duration:F1} s]");
            }
            Console.WriteLine();
        }

        // Display failed tests
        if (failed.Count > 0)
        {
            Console.WriteLine($"❌ FAILED TESTS ({failed.Count}):");
            foreach (var test in failed.OrderBy(t => t.TestName))
            {
                Console.WriteLine($"  Failed {test.TestName} [{test.Duration:F1} s]");
                if (!string.IsNullOrEmpty(test.ErrorMessage))
                {
                    Console.WriteLine($"    Error: {test.ErrorMessage}");
                }
            }
            Console.WriteLine();
        }

        // Display skipped tests
        if (skipped.Count > 0)
        {
            Console.WriteLine($"⏭️  SKIPPED TESTS ({skipped.Count}):");
            foreach (var test in skipped.OrderBy(t => t.TestName))
            {
                Console.WriteLine($"  Skipped {test.TestName}");
                if (!string.IsNullOrEmpty(test.SkipReason))
                {
                    Console.WriteLine($"    Reason: {test.SkipReason}");
                }
            }
            Console.WriteLine();
        }

        // Display overall summary
        var totalTests = results.Count;
        var totalDuration = results.Sum(r => r.Duration);
        
        Console.WriteLine($"TOTAL: {totalTests} tests, {passed.Count} passed, {failed.Count} failed, {skipped.Count} skipped");
        Console.WriteLine($"DURATION: {totalDuration:F1} seconds");
        Console.WriteLine("==========================================");
        Console.WriteLine();
    }

    public static void Reset()
    {
        lock (Lock)
        {
            _summaryDisplayed = false;
            while (TestResults.TryDequeue(out _)) { }
        }
    }
}

public class TestResult
{
    public string TestName { get; set; } = string.Empty;
    public TestStatus Status { get; set; }
    public double Duration { get; set; }
    public string? ErrorMessage { get; set; }
    public string? SkipReason { get; set; }
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
}

public enum TestStatus
{
    Passed,
    Failed,
    Skipped
}