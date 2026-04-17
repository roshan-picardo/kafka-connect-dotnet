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
        LogMessage("Initializing Kafka Connect distributed worker...", "");
        
        await CreateContainersAsync();
        
        await Task.Delay(10000);
        
        var distributedConfig = Configuration.TestContainers.Containers.FirstOrDefault(c => c.Target == "distributed");
        if (distributedConfig?.WaitForHealthCheck == true)
        {
            var distributedEndpoint = Configuration.GetServiceEndpoint("Distributed");
            var statusUrl = $"{distributedEndpoint}/workers/status";
            await WaitForWorkerReadyAsync(statusUrl, "Distributed worker");
        }
        
        LogMessage("Kafka Connect distributed worker initialized!", "");
    }
}
