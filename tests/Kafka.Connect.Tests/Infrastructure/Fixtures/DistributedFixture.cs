using DotNet.Testcontainers.Networks;

namespace IntegrationTests.Kafka.Connect.Infrastructure.Fixtures;

public class DistributedFixture(
    TestConfiguration configuration,
    Action<string, string> logMessage,
    IContainerService containerService,
    INetwork network)
    : InfrastructureFixture(configuration, logMessage, containerService, network)
{
    protected override string GetTargetName() => "distributed";

    public override async Task InitializeAsync()
    {
        await CreateContainersAsync();
        
        await Task.Delay(ReadyDelayMs);
        
        var distributedConfig = Configuration.TestContainers.Containers.FirstOrDefault(c => c.Target == "distributed");
        if (distributedConfig?.WaitForHealthCheck == true)
        {
            var distributedEndpoint = Configuration.GetServiceEndpoint("Distributed");
            var statusUrl = $"{distributedEndpoint}/workers/status";
            await WaitForWorkerReadyAsync(statusUrl, $"{GetTargetName()} worker");
        }
        
        LogMessage($"Kafka Connect {GetTargetName()} worker is ready.", "");
    }
}
