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
        LogMessage("Initializing Kafka Connect leader...", "");
        
        await CreateContainersAsync();
        
        await Task.Delay(10000);
        
        var leaderConfig = Configuration.TestContainers.Containers.FirstOrDefault(c => c.Target == "leader");
        if (leaderConfig?.WaitForHealthCheck == true)
        {
            var leaderEndpoint = Configuration.GetServiceEndpoint("Leader");
            var statusUrl = $"{leaderEndpoint}/workers/status";
            await WaitForWorkerReadyAsync(statusUrl, "Leader");
        }
        
        // Submit distributed connector configurations to the leader
        await SubmitDistributedConnectorsAsync();
        
        LogMessage("Kafka Connect leader initialized!", "");
    }

    private async Task SubmitDistributedConnectorsAsync()
    {
        LogMessage("Submitting distributed connector configurations to leader...", "");
        
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
        
        // Submit all connector configurations
        var connectorNames = configFiles.Select(Path.GetFileNameWithoutExtension).ToList();
        var successCount = await SubmitConnectorConfigurationsAsync(connectorNames!, configDirectory, "submitted");
        
        // Wait for distributed worker and connectors to be ready with retry logic
        if (successCount > 0)
        {
            LogMessage("Waiting for distributed worker and connectors to be ready...", "");
            await Task.Delay(10000); // Initial delay before checking
            
            var distributedEndpoint = Configuration.GetServiceEndpoint("Distributed");
            var statusUrl = $"{distributedEndpoint}/workers/status";
            
            // Pass retry callback to resubmit failed connectors
            await WaitForWorkerReadyAsync(statusUrl, "Distributed worker", RetryFailedConnectorsAsync);
        }
    }

    public async Task RetryFailedConnectorsAsync(List<string> failedConnectorNames)
    {
        if (failedConnectorNames.Count == 0)
        {
            return;
        }

        LogMessage($"Retrying submission for failed connectors: [{string.Join(", ", failedConnectorNames)}]", "");
        
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
                    LogMessage($"Successfully {actionVerb} connector configuration: {connectorName}", "");
                    
                    // Add delay to allow worker to process and flush config file to disk
                    // This prevents race conditions where the worker tries to reload configs
                    // before the file system has fully synced the writes
                    await Task.Delay(1000);
                }
                else
                {
                    failureCount++;
                    var errorContent = await response.Content.ReadAsStringAsync();
                    LogMessage($"Failed to {actionVerb.TrimEnd('d')} connector configuration {connectorName}: HTTP {(int)response.StatusCode} - {errorContent}", "");
                }
            }
            catch (Exception ex)
            {
                failureCount++;
                LogMessage($"Error {actionVerb.TrimEnd('d')}ting connector configuration {connectorName}: {ex.Message}", "");
            }
        }
        
        LogMessage($"Connector {actionVerb.TrimEnd('d')}sion completed: {successCount} succeeded, {failureCount} failed", "");
        
        return successCount;
    }
}
