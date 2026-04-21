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
    protected readonly TestCaseConfig[]? TestConfigs = testConfigs;

    public override async Task InitializeAsync()
    {
        var targetName = GetTargetName();
        await CreateContainersAsync();
        
        await WaitForReadyAsync();
        
        var testConfig = TestConfigs?.FirstOrDefault(tc =>
            tc.Target?.Equals(targetName, StringComparison.OrdinalIgnoreCase) == true);
            
        if (testConfig?.Setup?.Scripts is { Length: > 0 })
        {
            await ExecuteScriptsAsync(testConfig.Setup.Database, testConfig.Setup.Scripts);
        }

        LogMessage($"Infrastructure is ready: {GetTargetName()}", "");
    }

    protected abstract Task WaitForReadyAsync();
    protected abstract Task ExecuteScriptsAsync(string database, string[] scripts);
}
