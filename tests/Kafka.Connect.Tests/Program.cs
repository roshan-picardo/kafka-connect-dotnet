using System.Text.Json;
using IntegrationTests.Kafka.Connect.Infrastructure;

var fixture = new TestFixture();

fixture.LogMessage("=== Kafka Connect Debug Mode ===");

try
{
    fixture.Configuration.DebugMode = true;
    await InitializeInfrastructure();
    
    fixture.LogMessage("Available services:");
    foreach (var service in fixture.Configuration.Services)
    {
        fixture.LogMessage($"{service.Key}: {service.Value}");
    }
    
    fixture.LogMessage("Interactive Mode:");
    fixture.LogMessage("- Press ENTER to open test case selector");
    fixture.LogMessage("- Press ESC to exit debug mode");
    fixture.LogMessage("- Or type commands: 'select', 'exit'");

    await RunInteractiveMode();
}
catch (Exception ex)
{
    fixture.LogMessage($"Error: {ex.Message}");
    fixture.LogMessage($"Stack trace: {ex.StackTrace}");
}
finally
{
    await fixture.DisposeAsync();
}

return;

async Task InitializeInfrastructure()
{
    Console.WriteLine("Initializing test infrastructure...");
    
    try
    {
        fixture.LogMessage("Starting TestFixture initialization...");
        await fixture.InitializeAsync();
        fixture.LogMessage("TestFixture initialization completed.");
    }
    catch (Exception ex)
    {
        fixture.LogMessage($"Error during initialization: {ex.Message}");
        fixture.LogMessage($"Stack trace: {ex.StackTrace}");
        throw;
    }
}

async Task RunInteractiveMode()
{
    while (true)
    {
        fixture.LogMessage("Press ENTER to select test case, ESC to exit, or type 'select'/'exit': ");
        var keyInfo = Console.ReadKey(true);
        switch (keyInfo.Key)
        {
            case ConsoleKey.Enter:
                try
                {
                    var selectedTestCase = SelectTestCaseInteractively();
                    if (!string.IsNullOrEmpty(selectedTestCase))
                    {
                        await ExecuteTestCase(selectedTestCase);
                    }
                }
                catch (Exception ex)
                {
                    fixture.LogMessage($"Error executing test case: {ex.Message}");
                }
                break;
                
            case ConsoleKey.Escape:
                fixture.LogMessage("Exiting debug mode...");
                return;
                
            default:
                Console.Write(keyInfo.KeyChar);
                var input = Console.ReadLine();
                var fullInput = (keyInfo.KeyChar + input).Trim();
                
                if (string.IsNullOrEmpty(fullInput) || fullInput.Equals("exit", StringComparison.OrdinalIgnoreCase))
                {
                    return;
                }

                var parts = fullInput.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                var command = parts[0].ToLowerInvariant();

                try
                {
                    switch (command)
                    {
                        case "select":
                            var selectedTestCase = SelectTestCaseInteractively();
                            if (!string.IsNullOrEmpty(selectedTestCase))
                            {
                                await ExecuteTestCase(selectedTestCase);
                            }
                            break;
                        
                        default:
                            fixture.LogMessage($"Unknown command: {command}");
                            fixture.LogMessage("Available commands: select, exit");
                            break;
                    }
                }
                catch (Exception ex)
                {
                    fixture.LogMessage($"Error executing command: {ex.Message}");
                }
                break;
        }
    }

    async Task ExecuteTestCase(string fileName)
    {
        try
        {
            var configPath = Path.Combine("data", "test-config.json");
            var configContent = File.ReadAllText(configPath);
            var configs = JsonSerializer.Deserialize<TestCaseConfig[]>(configContent, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
            
            var target = (from config in configs?.Where(c => !c.Skip) where config.Files?.Any(f => Path.GetFileNameWithoutExtension(f.TrimStart('/')) == Path.GetFileNameWithoutExtension(fileName)) == true select config.Target).FirstOrDefault();

            var filePath = Path.Combine("data/records", fileName.EndsWith(".json") ? fileName : $"{fileName}.json");
            var content = await File.ReadAllTextAsync(filePath);
            var testCase = JsonSerializer.Deserialize<TestCase>(content, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
            
            if (testCase == null)
            {
                fixture.LogMessage("Failed to deserialize test case");
                return;
            }

            if (!string.IsNullOrEmpty(target))
            {
                testCase.Properties.TryAdd("target", target);
            }

            fixture.LogMessage($"Test Case: {testCase.Title}, Records: {testCase.Records.Length}, Target: {target ?? "Unknown"}");

            for (var i = 0; i < testCase.Records.Length; i++)
            {
                var record = testCase.Records[i];
                fixture.LogMessage($"Executing: {testCase.Title} - {record.Operation}" ,$"Record: {i + 1}/{testCase.Records.Length}");
                if (testCase.Records.Length > 1)
                {
                    if (i < testCase.Records.Length - 1)
                    {
                        testCase.Properties.TryAdd("skip", "cleanup");
                    }

                    if (i > 0)
                    {
                        testCase.Properties.TryAdd("skip", "setup");
                    }
                }
                
                await ExecuteSingleRecord(testCase, record);
                if (i < testCase.Records.Length - 1)
                {
                    Console.ReadKey();
                }
            }
            
            fixture.LogMessage($"Test case '{testCase.Title}' completed successfully!");
        }
        catch (Exception ex)
        {
            fixture.LogMessage($"Error executing test case: {ex.Message}");
            fixture.LogMessage($"Stack trace: {ex.StackTrace}");
        }
    }

    async Task ExecuteSingleRecord(TestCase testCase, TestCaseRecord record)
    {
        try
        {
            var target = testCase.Properties.TryGetValue("target", out var targetValue) ? targetValue : "Unknown";
            
            var singleRecordTestCase = new TestCase(
                $"{testCase.Title} - {record.Operation}",
                testCase.Properties,
                [record],
                testCase.Skip
            );

            if (record.Delay > 0)
            {
                await Task.Delay(record.Delay);
            }

            var testOutput = new ConsoleTestOutputHelper();
            
            switch (target.ToLowerInvariant())
            {
                case "mongo":
                    var mongoRunner = new IntegrationTests.Kafka.Connect.MongoTestRunner(fixture, testOutput);
                    await mongoRunner.Execute(singleRecordTestCase);
                    break;
                    
                case "postgres":
                    var postgresRunner = new IntegrationTests.Kafka.Connect.PostgresTestRunner(fixture, testOutput);
                    await postgresRunner.Execute(singleRecordTestCase);
                    break;
                    
                case "sqlserver":
                    var sqlServerRunner = new IntegrationTests.Kafka.Connect.SqlServerTestRunner(fixture, testOutput);
                    await sqlServerRunner.Execute(singleRecordTestCase);
                    break;
                    
                case "mysql":
                    var mySqlRunner = new IntegrationTests.Kafka.Connect.MySqlTestRunner(fixture, testOutput);
                    await mySqlRunner.Execute(singleRecordTestCase);
                    break;
                    
                case "health":
                    var healthRunner = new IntegrationTests.Kafka.Connect.HealthTestRunner(fixture, testOutput);
                    await healthRunner.Execute(singleRecordTestCase);
                    break;
                    
                default:
                    fixture.LogMessage($"Unknown target: {target}. Executing as publish operation.");
                    break;
            }
        }
        catch (Exception ex)
        {
            fixture.LogMessage($"Error executing record: {ex.Message}");
            throw;
        }
    }

    string? SelectTestCaseInteractively()
    {
        try
        {
            var configPath = Path.Combine("data", "test-config.json");
            if (!File.Exists(configPath))
            {
                fixture.LogMessage("test-config.json not found");
                return null;
            }
            
            var configContent = File.ReadAllText(configPath);
            var configs = JsonSerializer.Deserialize<TestCaseConfig[]>(configContent, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
            
            if (configs == null || configs.Length == 0)
            {
                fixture.LogMessage("No test case configurations found");
                return null;
            }
            
            var testCases = (from config in configs.Where(c => !c.Skip) let target = config.Target ?? "Unknown" where config.Files?.Any() == true from file in config.Files let fileName = Path.GetFileNameWithoutExtension(file.TrimStart('/')) select (fileName, target)).ToList();

            if (testCases.Count == 0)
            {
                fixture.LogMessage("No test cases found");
                return null;
            }
            
            var selectedIndex = 0;
            ConsoleKeyInfo keyInfo;
            
            do
            {
                Console.Clear();
                fixture.LogMessage("Use ↑↓ arrows, Enter to select, Esc to cancel", "TestCase");
                
                for (int i = 0; i < testCases.Count; i++)
                {
                    var (fileName, target) = testCases[i];
                    var prefix = i == selectedIndex ? "► " : "  ";
                    var line = $" {prefix} {fileName}";
                    
                    if (i == selectedIndex)
                    {
                        Console.BackgroundColor = ConsoleColor.DarkGreen;
                        Console.ForegroundColor = ConsoleColor.White;
                    }
                    
                    fixture.LogMessage(line, target);
                    
                    if (i == selectedIndex)
                    {
                        Console.ResetColor();
                    }
                }
                
                keyInfo = Console.ReadKey(true);
                
                switch (keyInfo.Key)
                {
                    case ConsoleKey.UpArrow:
                        selectedIndex = selectedIndex > 0 ? selectedIndex - 1 : testCases.Count - 1;
                        break;
                        
                    case ConsoleKey.DownArrow:
                        selectedIndex = selectedIndex < testCases.Count - 1 ? selectedIndex + 1 : 0;
                        break;
                }
                
            } while (keyInfo.Key != ConsoleKey.Enter && keyInfo.Key != ConsoleKey.Escape);
            
            Console.Clear();
            
            if (keyInfo.Key == ConsoleKey.Enter)
            {
                return testCases[selectedIndex].fileName;
            }
            
            return null;
        }
        catch (Exception ex)
        {
            fixture.LogMessage($"Error in interactive selection: {ex.Message}");
            return null;
        }
    }
}