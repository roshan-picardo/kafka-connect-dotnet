using DotNet.Testcontainers.Networks;

namespace IntegrationTests.Kafka.Connect.Infrastructure.Fixtures;

public abstract class DatabaseFixture : InfrastructureFixture
{
    protected const int DatabaseReadyMaxAttempts = 60;
    protected const int DatabaseReadyDelayMs = 1000;
    protected readonly TestCaseConfig[]? TestConfigs;

    protected DatabaseFixture(
        TestConfiguration configuration,
        Action<string, string> logMessage,
        IContainerService containerService,
        INetwork network,
        TestCaseConfig[]? testConfigs)
        : base(configuration, logMessage, containerService, network)
    {
        TestConfigs = testConfigs;
    }

    public override async Task InitializeAsync()
    {
        var targetName = GetTargetName();
        LogMessage($"Initializing {targetName} infrastructure...", "");
        
        // Create database container
        await CreateContainersAsync();
        
        // Wait for database to be ready
        await WaitForReadyAsync();
        
        // Execute setup scripts if test configurations are available
        if (TestConfigs != null)
        {
            var testConfig = TestConfigs.FirstOrDefault(tc =>
                tc.Target?.Equals(targetName, StringComparison.OrdinalIgnoreCase) == true);
            
            if (testConfig?.Setup?.Scripts != null && testConfig.Setup.Scripts.Length > 0)
            {
                LogMessage($"Executing setup scripts for {targetName}...", "");
                await ExecuteScriptsAsync(testConfig.Setup.Database ?? string.Empty, testConfig.Setup.Scripts);
                LogMessage($"Setup scripts executed for {targetName}!", "");
            }
        }
        
        LogMessage($"{targetName} infrastructure initialized!", "");
    }

    public abstract Task WaitForReadyAsync();
    public abstract Task ExecuteScriptsAsync(string database, string[] scripts);
}
