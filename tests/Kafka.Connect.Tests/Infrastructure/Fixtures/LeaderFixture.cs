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
    private const int LeaderReadyMaxAttempts = 60;
    private const int LeaderReadyDelayMs = 1000;

    protected override string GetTargetName() => "leader";

    public override async Task InitializeAsync()
    {
        LogMessage("Initializing Kafka Connect leader...", "");
        
        await CreateContainersAsync();
        
        await Task.Delay(10000);
        
        var leaderConfig = Configuration.TestContainers.Containers.FirstOrDefault(c => c.Target == "leader");
        if (leaderConfig?.WaitForHealthCheck == true)
        {
            await WaitForReadyAsync();
        }
        
        // Submit distributed connector configurations to the leader
        await SubmitDistributedConnectorsAsync();
        
        LogMessage("Kafka Connect leader initialized!", "");
    }

    private async Task WaitForReadyAsync()
    {
        var leaderEndpoint = Configuration.GetServiceEndpoint("Leader");
        var statusUrl = $"{leaderEndpoint}/workers/status";

        using var httpClient = new HttpClient
        {
            Timeout = TimeSpan.FromSeconds(15)
        };

        for (var attempt = 1; attempt <= LeaderReadyMaxAttempts; attempt++)
        {
            try
            {
                var response = await httpClient.GetAsync(statusUrl);

                if (response.IsSuccessStatusCode)
                {
                    var content = await response.Content.ReadAsStringAsync();

                    try
                    {
                        var statusDoc = JsonDocument.Parse(content);

                        if (statusDoc.RootElement.TryGetProperty("status", out var status))
                        {
                            // Check leader status
                            var leaderRunning = false;
                            if (status.TryGetProperty("worker", out var worker) &&
                                worker.TryGetProperty("status", out var leaderStatus))
                            {
                                leaderRunning = leaderStatus.GetString() == "Running";
                            }

                            // Check all connectors are running
                            var allConnectorsRunning = true;
                            var connectorStatuses = new List<string>();

                            if (status.TryGetProperty("connectors", out var connectors))
                            {
                                foreach (var connector in connectors.EnumerateArray())
                                {
                                    if (connector.TryGetProperty("name", out var name) &&
                                        connector.TryGetProperty("status", out var connectorStatus))
                                    {
                                        var connectorName = name.GetString();
                                        var connectorStatusValue = connectorStatus.GetString();
                                        connectorStatuses.Add($"{connectorName}={connectorStatusValue}");

                                        if (connectorStatusValue != "Running")
                                        {
                                            allConnectorsRunning = false;
                                        }
                                    }
                                }
                            }

                            if (leaderRunning && allConnectorsRunning && connectorStatuses.Count > 0)
                            {
                                LogMessage(
                                    $"Leader and connectors are ready (attempt {attempt}): Leader=Running, Connectors=[{string.Join(", ", connectorStatuses)}]",
                                    "");
                                return;
                            }

                            LogMessage(
                                $"Leader or connectors not ready yet (attempt {attempt}/{LeaderReadyMaxAttempts}): Leader={leaderRunning}, Connectors=[{string.Join(", ", connectorStatuses)}]",
                                "");
                        }
                        else
                        {
                            LogMessage(
                                $"Leader response missing 'status' property (attempt {attempt}/{LeaderReadyMaxAttempts}): {content}",
                                "");
                        }
                    }
                    catch (JsonException ex)
                    {
                        LogMessage(
                            $"Failed to parse leader status JSON (attempt {attempt}/{LeaderReadyMaxAttempts}): {ex.Message}",
                            "");
                    }
                }
                else
                {
                    LogMessage(
                        $"Leader not ready yet (attempt {attempt}/{LeaderReadyMaxAttempts}): HTTP {(int)response.StatusCode}",
                        "");
                }
            }
            catch (HttpRequestException ex)
            {
                var errorType = ex.InnerException?.GetType().Name ?? ex.GetType().Name;
                LogMessage(
                    $"Leader endpoint not available yet (attempt {attempt}/{LeaderReadyMaxAttempts}): {errorType}", "");
            }
            catch (TaskCanceledException ex) when (ex.InnerException is TimeoutException)
            {
                LogMessage($"Leader health check timeout (attempt {attempt}/{LeaderReadyMaxAttempts})", "");
            }
            catch (Exception ex)
            {
                if (attempt == LeaderReadyMaxAttempts)
                {
                    throw new TimeoutException(
                        $"Leader did not become ready after {LeaderReadyMaxAttempts} attempts", ex);
                }

                LogMessage(
                    $"Leader not ready yet (attempt {attempt}/{LeaderReadyMaxAttempts}): {ex.GetType().Name} - {ex.Message}",
                    "");
            }

            await Task.Delay(LeaderReadyDelayMs);
        }

        throw new TimeoutException(
            $"Leader and connectors did not reach 'Running' status after {LeaderReadyMaxAttempts} attempts");
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
        
        var leaderEndpoint = Configuration.GetServiceEndpoint("Leader");
        
        using var httpClient = new HttpClient
        {
            Timeout = TimeSpan.FromSeconds(30)
        };
        
        var successCount = 0;
        var failureCount = 0;
        
        foreach (var configFile in configFiles)
        {
            try
            {
                var fileName = Path.GetFileNameWithoutExtension(configFile);
                var configContent = await File.ReadAllTextAsync(configFile);
                
                var postUrl = $"{leaderEndpoint}/connectors/{fileName}";
                var content = new StringContent(configContent, Encoding.UTF8, "application/json");
                
                var response = await httpClient.PostAsync(postUrl, content);
                
                if (response.IsSuccessStatusCode)
                {
                    successCount++;
                    LogMessage($"Successfully submitted connector configuration: {fileName}", "");
                    
                    // Add delay to allow worker to process and flush config file to disk
                    // This prevents race conditions where the worker tries to reload configs
                    // before the file system has fully synced the writes
                    await Task.Delay(1000);
                }
                else
                {
                    failureCount++;
                    var errorContent = await response.Content.ReadAsStringAsync();
                    LogMessage($"Failed to submit connector configuration {fileName}: HTTP {(int)response.StatusCode} - {errorContent}", "");
                }
            }
            catch (Exception ex)
            {
                failureCount++;
                LogMessage($"Error submitting connector configuration {Path.GetFileName(configFile)}: {ex.Message}", "");
            }
        }
        
        LogMessage($"Connector submission completed: {successCount} succeeded, {failureCount} failed", "");
        
        // Fetch and log distributed worker status
        if (successCount > 0)
        {
            LogMessage("Waiting 30 seconds for connectors to start...", "");
            await Task.Delay(30000);
            await LogDistributedWorkerStatusAsync(httpClient);
        }
    }

    private async Task LogDistributedWorkerStatusAsync(HttpClient httpClient)
    {
        try
        {
            var distributedEndpoint = Configuration.GetServiceEndpoint("Distributed");
            var statusUrl = $"{distributedEndpoint}/workers/status";
            
            LogMessage("Fetching distributed worker status...", "");
            
            var response = await httpClient.GetAsync(statusUrl);
            
            if (response.IsSuccessStatusCode)
            {
                var content = await response.Content.ReadAsStringAsync();
                try
                {
                    var statusDoc = JsonDocument.Parse(content);
                    
                    if (statusDoc.RootElement.TryGetProperty("status", out var status))
                    {
                        // Log worker status
                        if (status.TryGetProperty("worker", out var worker))
                        {
                            var workerStatus = worker.TryGetProperty("status", out var ws) ? ws.GetString() : "Unknown";
                            var workerName = worker.TryGetProperty("name", out var wn) ? wn.GetString() : "Unknown";
                            LogMessage($"Distributed Worker: {workerName} - Status: {workerStatus}", "");
                        }
                        
                        // Log connector statuses
                        if (status.TryGetProperty("connectors", out var connectors))
                        {
                            var connectorList = new List<string>();
                            foreach (var connector in connectors.EnumerateArray())
                            {
                                if (connector.TryGetProperty("name", out var name) &&
                                    connector.TryGetProperty("status", out var connectorStatus))
                                {
                                    var connectorName = name.GetString();
                                    var connectorStatusValue = connectorStatus.GetString();
                                    connectorList.Add($"{connectorName}={connectorStatusValue}");
                                }
                            }
                            
                            if (connectorList.Count > 0)
                            {
                                LogMessage($"Distributed Connectors: [{string.Join(", ", connectorList)}]", "");
                            }
                            else
                            {
                                LogMessage("Distributed Connectors: None registered yet", "");
                            }
                        }
                    }
                    else
                    {
                        LogMessage($"Distributed worker status response missing 'status' property: {content}", "");
                    }
                }
                catch (JsonException ex)
                {
                    LogMessage($"Failed to parse distributed worker status JSON: {ex.Message}", "");
                }
            }
            else
            {
                LogMessage($"Failed to fetch distributed worker status: HTTP {(int)response.StatusCode}", "");
            }
        }
        catch (Exception ex)
        {
            LogMessage($"Error fetching distributed worker status: {ex.Message}", "");
        }
        
        // Pause execution to allow inspection
        // LogMessage("Press Enter to continue...", "");
        // Console.ReadLine();
    }
}
