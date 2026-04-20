using DotNet.Testcontainers.Networks;

namespace IntegrationTests.Kafka.Connect.Infrastructure.Fixtures;

public class WorkerFixture(
    TestConfiguration configuration,
    Action<string, string> logMessage,
    IContainerService containerService,
    INetwork network)
    : InfrastructureFixture(configuration, logMessage, containerService, network)
{
    protected override string GetTargetName() => "worker";

    public override async Task InitializeAsync()
    {
        LogMessage("Initializing Kafka Connect worker...", "");
        
        await CreateContainersAsync();
        
        await Task.Delay(10000);
        
        var workerConfig = Configuration.TestContainers.Containers.FirstOrDefault(c => c.Target == "worker");
        if (workerConfig?.WaitForHealthCheck == true)
        {
            var workerEndpoint = Configuration.GetServiceEndpoint("Worker");
            var statusUrl = $"{workerEndpoint}/workers/status";
            await WaitForWorkerReadyAsync(statusUrl, "Worker");
        }
        
        LogMessage("Kafka Connect worker initialized!", "");
    }
}
