using System.Text.Json;
using IntegrationTests.Kafka.Connect;
using IntegrationTests.Kafka.Connect.Infrastructure;
using Xunit.Abstractions;

var fixture = new TestFixture();
fixture.Configuration.DebugMode = true;

try
{
    fixture.LogMessage("=== Kafka Connect Debug Runner ===");
    fixture.LogMessage("Starting infrastructure (Kafka, databases)...");
    await fixture.InitializeAsync();
    fixture.LogMessage("Infrastructure ready. Press ENTER to pick a test case, ESC to exit.");

    await RunInteractiveLoop();
}
catch (Exception ex)
{
    fixture.LogMessage($"Fatal: {ex.Message}");
    fixture.LogMessage(ex.StackTrace ?? "");
}
finally
{
    await fixture.DisposeAsync();
}

return;

async Task RunInteractiveLoop()
{
    while (true)
    {
        var key = Console.ReadKey(true);

        if (key.Key == ConsoleKey.Escape)
        {
            fixture.LogMessage("Exiting...");
            return;
        }

        if (key.Key != ConsoleKey.Enter)
            continue;

        var testCase = SelectTestCase();
        if (testCase == null) continue;

        await RunTestCase(testCase);
    }
}

TestCase? SelectTestCase()
{
    var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
    var configs = JsonSerializer.Deserialize<TestCaseConfig[]>(
        File.ReadAllText(Path.Combine("data", "test-config.json")), options) ?? [];

    var activeTargets = fixture.Configuration.Targets.Count > 0
        ? fixture.Configuration.Targets.Select(t => t.ToLowerInvariant()).ToHashSet()
        : null;

    var allCases = new List<(TestCase testCase, string target)>();

    foreach (var config in configs.Where(c => !c.Skip && c.Target != null && c.Files?.Length > 0))
    {
        if (activeTargets != null && !activeTargets.Contains(config.Target!.ToLowerInvariant()))
            continue;

        foreach (var file in config.Files!)
        {
            var filePath = $"data/{file.TrimStart('/')}";
            if (!File.Exists(filePath)) continue;

            var testCase = JsonSerializer.Deserialize<TestCase>(File.ReadAllText(filePath), options);
            if (testCase == null) continue;

            testCase.Properties.TryAdd("target", config.Target!);
            allCases.Add((testCase, config.Target!));
        }
    }

    if (allCases.Count == 0)
    {
        fixture.LogMessage("No test cases found.");
        return null;
    }

    var selectedIndex = 0;
    ConsoleKeyInfo key;

    do
    {
        Console.Clear();
        fixture.LogMessage("↑↓ to navigate, ENTER to run, ESC to cancel", "Select");

        for (var i = 0; i < allCases.Count; i++)
        {
            var (tc, targetLabel) = allCases[i];
            var prefix = i == selectedIndex ? "► " : "  ";

            if (i == selectedIndex)
            {
                Console.BackgroundColor = ConsoleColor.DarkGreen;
                Console.ForegroundColor = ConsoleColor.White;
            }

            fixture.LogMessage($"{prefix}{tc.Title}", targetLabel);

            if (i == selectedIndex)
                Console.ResetColor();
        }

        key = Console.ReadKey(true);

        switch (key.Key)
        {
            case ConsoleKey.UpArrow:
                selectedIndex = selectedIndex > 0 ? selectedIndex - 1 : allCases.Count - 1;
                break;
            case ConsoleKey.DownArrow:
                selectedIndex = selectedIndex < allCases.Count - 1 ? selectedIndex + 1 : 0;
                break;
        }
    } while (key.Key != ConsoleKey.Enter && key.Key != ConsoleKey.Escape);

    Console.Clear();
    return key.Key == ConsoleKey.Enter ? allCases[selectedIndex].testCase : null;
}

async Task RunTestCase(TestCase testCase)
{
    var target = testCase.Properties.GetValueOrDefault("target", "Unknown");
    fixture.LogMessage($"Running: {testCase.Title} ({testCase.Records.Length} records)", target);

    var runner = new TestRunnerBaseHealthChecks(fixture, new ConsoleTestOutputHelper());

    for (var i = 0; i < testCase.Records.Length; i++)
    {
        var record = testCase.Records[i];
        fixture.LogMessage($"[{i + 1}/{testCase.Records.Length}] {record.Operation} — press any key to execute, ESC to abort", target);

        if (Console.ReadKey(true).Key == ConsoleKey.Escape) return;

        try
        {
            await runner.Execute(testCase with { Records = [record] });
            fixture.LogMessage($"[{i + 1}/{testCase.Records.Length}] {record.Operation} OK", target);
        }
        catch (Exception ex)
        {
            fixture.LogMessage($"[{i + 1}/{testCase.Records.Length}] {record.Operation} FAILED: {ex.Message}", target);
            fixture.LogMessage("Press any key to continue to next record, ESC to abort", target);
            if (Console.ReadKey(true).Key == ConsoleKey.Escape) return;
        }
    }

    fixture.LogMessage($"Completed: {testCase.Title}", target);
}

class ConsoleTestOutputHelper : ITestOutputHelper
{
    public void WriteLine(string message) => Console.WriteLine(message);
    public void WriteLine(string format, params object[] args) => Console.WriteLine(format, args);
}
