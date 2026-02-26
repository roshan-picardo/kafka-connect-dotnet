using DotNet.Testcontainers.Networks;

namespace IntegrationTests.Kafka.Connect.Infrastructure.Fixtures;

public abstract class DatabaseFixture(
    TestConfiguration configuration,
    Action<string, string> logMessage,
    IContainerService containerService,
    INetwork network,
    TestCaseConfig[]? testConfigs)
    : InfrastructureFixture(configuration, logMessage, containerService, network)
{
    protected const int DatabaseReadyMaxAttempts = 60;
    protected const int DatabaseReadyDelayMs = 1000;
    protected readonly TestCaseConfig[]? TestConfigs = testConfigs;

    public override async Task InitializeAsync()
    {
        var targetName = GetTargetName();
        LogMessage($"Initializing {targetName}...", "");
        
        await CreateContainersAsync();
        
        await WaitForReadyAsync();
        
        var testConfig = TestConfigs?.FirstOrDefault(tc =>
            tc.Target?.Equals(targetName, StringComparison.OrdinalIgnoreCase) == true);
            
        if (testConfig?.Setup?.Scripts is { Length: > 0 })
        {
            await ExecuteScriptsAsync(testConfig.Setup.Database ?? string.Empty, testConfig.Setup.Scripts);
            LogMessage($"Setup scripts executed for {targetName}!", "");
        }

        LogMessage($"{targetName} initialized!", "");
    }

    protected abstract Task WaitForReadyAsync();
    protected abstract Task ExecuteScriptsAsync(string database, string[] scripts);
}
