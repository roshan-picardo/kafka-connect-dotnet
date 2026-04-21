using DotNet.Testcontainers.Networks;

namespace IntegrationTests.Kafka.Connect.Infrastructure.Fixtures;

public class StandaloneFixture(
    TestConfiguration configuration,
    Action<string, string> logMessage,
    IContainerService containerService,
    INetwork network)
    : InfrastructureFixture(configuration, logMessage, containerService, network)
{
    protected override string GetTargetName() => "standalone";

    public override async Task InitializeAsync()
    {
        await CreateContainersAsync();
        
        await Task.Delay(10000);
        
        var standaloneConfig = Configuration.TestContainers.Containers.FirstOrDefault(c => c.Target == "standalone");
        if (standaloneConfig?.WaitForHealthCheck == true)
        {
            var standaloneEndpoint = Configuration.GetServiceEndpoint("Standalone");
            var statusUrl = $"{standaloneEndpoint}/workers/status";
            await WaitForWorkerReadyAsync(statusUrl, "Standalone worker");
        }
        
        LogMessage($"Kafka Connect {GetTargetName()} worker is ready.", "");
    }
}
