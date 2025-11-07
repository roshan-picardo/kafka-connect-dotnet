using System.Collections.Concurrent;
using System.Text.RegularExpressions;
using Xunit.Abstractions;

namespace IntegrationTests.Kafka.Connect.Infrastructure;

public class TestResultCollector
{
    private static readonly ConcurrentQueue<TestResult> TestResults = new();
    private static readonly object Lock = new();
    private static bool _summaryDisplayed = false;
    // Removed complex regex patterns - using simple string parsing instead

    public static void AddResult(TestResult result)
    {
        TestResults.Enqueue(result);
    }

    public static bool IsTestResultMessage(string message)
    {
        var trimmed = message.Trim();
        return (trimmed.StartsWith("Passed ") || trimmed.StartsWith("Failed ") || trimmed.StartsWith("Skipped ")) &&
               trimmed.Contains("[") && (trimmed.Contains("ms]") || trimmed.Contains("s]"));
    }

    public static void ParseAndAddResult(string logLine)
    {
        var trimmed = logLine.Trim();
        
        // Simple string-based parsing - much more reliable
        string status;
        string testName;
        double duration;
        
        if (trimmed.StartsWith("Passed "))
        {
            status = "Passed";
            var remaining = trimmed.Substring(7); // Remove "Passed "
            if (TryParseTestResult(remaining, out testName, out duration))
            {
                AddResult(new TestResult
                {
                    TestName = testName,
                    Status = TestStatus.Passed,
                    Duration = duration
                });
            }
        }
        else if (trimmed.StartsWith("Failed "))
        {
            status = "Failed";
            var remaining = trimmed.Substring(7); // Remove "Failed "
            if (TryParseTestResult(remaining, out testName, out duration))
            {
                AddResult(new TestResult
                {
                    TestName = testName,
                    Status = TestStatus.Failed,
                    Duration = duration
                });
            }
        }
        else if (trimmed.StartsWith("Skipped "))
        {
            status = "Skipped";
            var remaining = trimmed.Substring(8); // Remove "Skipped "
            if (TryParseTestResult(remaining, out testName, out duration))
            {
                AddResult(new TestResult
                {
                    TestName = testName,
                    Status = TestStatus.Skipped,
                    Duration = duration
                });
            }
        }
    }
    
    private static bool TryParseTestResult(string remaining, out string testName, out double duration)
    {
        testName = string.Empty;
        duration = 0.0;
        
        // Find the last occurrence of '[' to locate the duration
        var lastBracketIndex = remaining.LastIndexOf('[');
        if (lastBracketIndex == -1) return false;
        
        // Extract test name (everything before the last '[')
        testName = remaining.Substring(0, lastBracketIndex).Trim();
        
        // Extract duration part
        var durationPart = remaining.Substring(lastBracketIndex + 1);
        var closeBracketIndex = durationPart.IndexOf(']');
        if (closeBracketIndex == -1) return false;
        
        durationPart = durationPart.Substring(0, closeBracketIndex).Trim();
        
        // Parse duration and unit
        if (durationPart.EndsWith("ms"))
        {
            var durationStr = durationPart.Substring(0, durationPart.Length - 2).Trim();
            if (double.TryParse(durationStr, out var ms))
            {
                duration = ms / 1000.0; // Convert to seconds
                return true;
            }
        }
        else if (durationPart.EndsWith("s"))
        {
            var durationStr = durationPart.Substring(0, durationPart.Length - 1).Trim();
            if (double.TryParse(durationStr, out var s))
            {
                duration = s;
                return true;
            }
        }
        
        return false;
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