using DotNet.Testcontainers.Networks;
using System.Text;
using System.Text.Json;

namespace IntegrationTests.Kafka.Connect.Infrastructure.Fixtures;

public class LeaderFixture(
    TestConfiguration configuration,
    Action<string, string> logMessage,
    IContainerService containerService,
    INetwork network)
    : InfrastructureFixture(configuration, logMessage, containerService, network)
{
    protected override string GetTargetName() => "leader";

    public override async Task InitializeAsync()
    {
        await CreateContainersAsync();
        
        var leaderConfig = Configuration.TestContainers.Containers.FirstOrDefault(c => c.Target == "leader");
        if (leaderConfig?.WaitForHealthCheck == true)
        {
            var leaderEndpoint = Configuration.GetServiceEndpoint("Leader");
            var statusUrl = $"{leaderEndpoint}/workers/status";
            await WaitForWorkerReadyAsync(statusUrl, "leader");
        }
        
        await SubmitDistributedConnectorsAsync();
        
        LogMessage($"Kafka Connect {GetTargetName()} is ready.", "");
    }

    private async Task SubmitDistributedConnectorsAsync()
    {
        var configDirectory = Path.Join(Directory.GetCurrentDirectory(), "Configurations", "distributed");
        
        if (!Directory.Exists(configDirectory))
        {
            LogMessage("No distributed configuration directory found, skipping connector submission", "");
            return;
        }
        
        var configFiles = Directory.GetFiles(configDirectory, "*.json");
        
        if (configFiles.Length == 0)
        {
            LogMessage("No distributed connector configuration files found, skipping connector submission", "");
            return;
        }
        
        var connectorNames = configFiles.Select(Path.GetFileNameWithoutExtension).ToList();
        var successCount = await SubmitConnectorConfigurationsAsync(connectorNames!, configDirectory, "submitted");
        
        if (successCount > 0)
        {
            var distributedEndpoint = Configuration.GetServiceEndpoint("Distributed");
            var statusUrl = $"{distributedEndpoint}/workers/status";
            await WaitForWorkerReadyAsync(statusUrl, "distributed worker", RetryFailedConnectorsAsync);
        }
    }

    public async Task RetryFailedConnectorsAsync(List<string> failedConnectorNames)
    {
        if (failedConnectorNames.Count == 0)
        {
            return;
        }

        LogMessage($"Retrying failed connectors: [{string.Join(", ", failedConnectorNames)}]", "");
        
        var configDirectory = Path.Join(Directory.GetCurrentDirectory(), "Configurations", "distributed");
        await SubmitConnectorConfigurationsAsync(failedConnectorNames, configDirectory, "resubmitted");
    }

    private async Task<int> SubmitConnectorConfigurationsAsync(List<string> connectorNames, string configDirectory, string actionVerb)
    {
        var leaderEndpoint = Configuration.GetServiceEndpoint("Leader");
        
        using var httpClient = new HttpClient
        {
            Timeout = TimeSpan.FromSeconds(30)
        };
        
        var successCount = 0;
        var failureCount = 0;
        
        foreach (var connectorName in connectorNames)
        {
            try
            {
                var configFile = Path.Join(configDirectory, $"{connectorName}.json");
                
                if (!File.Exists(configFile))
                {
                    LogMessage($"Configuration file not found for connector: {connectorName}", "");
                    failureCount++;
                    continue;
                }
                
                var configContent = await File.ReadAllTextAsync(configFile);
                var postUrl = $"{leaderEndpoint}/connectors/{connectorName}";
                var content = new StringContent(configContent, Encoding.UTF8, "application/json");
                
                var response = await httpClient.PostAsync(postUrl, content);
                
                if (response.IsSuccessStatusCode)
                {
                    successCount++;
                    LogMessage($"Submitted connector configuration: {connectorName}", "");
                    await Task.Delay(500);
                }
                else
                {
                    failureCount++;
                    var errorContent = await response.Content.ReadAsStringAsync();
                    LogMessage($"Failed to submit connector configuration {connectorName}: HTTP {(int)response.StatusCode} - {errorContent}", "");
                }
            }
            catch (Exception ex)
            {
                failureCount++;
                LogMessage($"Error submitting connector configuration {connectorName}: {ex.Message}", "");
            }
        }
        
        LogMessage($"Configuration completed: {successCount} succeeded, {failureCount} failed", "");
        
        return successCount;
    }
}
