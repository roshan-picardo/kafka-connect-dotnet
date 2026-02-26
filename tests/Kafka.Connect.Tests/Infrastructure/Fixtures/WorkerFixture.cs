using DotNet.Testcontainers.Networks;
using System.Text.Json;

namespace IntegrationTests.Kafka.Connect.Infrastructure.Fixtures;

public class WorkerFixture : InfrastructureFixture
{
    private const int WorkerReadyMaxAttempts = 60;
    private const int WorkerReadyDelayMs = 1000;

    public WorkerFixture(
        TestConfiguration configuration,
        Action<string, string> logMessage,
        IContainerService containerService,
        INetwork network)
        : base(configuration, logMessage, containerService, network)
    {
    }

    protected override string GetTargetName() => "worker";

    public override async Task InitializeAsync()
    {
        LogMessage("Initializing Kafka Connect worker...", "");
        
        // Create worker container
        await CreateContainersAsync();
        
        // Wait a bit for worker to start
        await Task.Delay(10000);
        
        // Wait for worker to be ready
        var workerConfig = Configuration.TestContainers.Containers.FirstOrDefault(c => c.Target == "worker");
        if (workerConfig?.WaitForHealthCheck == true)
        {
            await WaitForReadyAsync();
        }
        
        LogMessage("Kafka Connect worker initialized!", "");
    }

    public async Task WaitForReadyAsync()
    {
        var workerEndpoint = Configuration.GetServiceEndpoint("Worker");
        var statusUrl = $"{workerEndpoint}/workers/status";

        using var httpClient = new HttpClient
        {
            Timeout = TimeSpan.FromSeconds(15)
        };

        for (var attempt = 1; attempt <= WorkerReadyMaxAttempts; attempt++)
        {
            try
            {
                var response = await httpClient.GetAsync(statusUrl);

                if (response.IsSuccessStatusCode)
                {
                    var content = await response.Content.ReadAsStringAsync();

                    // Parse the JSON response to check connector statuses
                    try
                    {
                        var statusDoc = JsonDocument.Parse(content);

                        if (statusDoc.RootElement.TryGetProperty("status", out var status))
                        {
                            // Check worker status
                            var workerRunning = false;
                            if (status.TryGetProperty("worker", out var worker) &&
                                worker.TryGetProperty("status", out var workerStatus))
                            {
                                workerRunning = workerStatus.GetString() == "Running";
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

                            if (workerRunning && allConnectorsRunning && connectorStatuses.Count > 0)
                            {
                                LogMessage(
                                    $"Worker and connectors are ready (attempt {attempt}): Worker=Running, Connectors=[{string.Join(", ", connectorStatuses)}]",
                                    "");
                                return;
                            }

                            LogMessage(
                                $"Worker or connectors not ready yet (attempt {attempt}/{WorkerReadyMaxAttempts}): Worker={workerRunning}, Connectors=[{string.Join(", ", connectorStatuses)}]",
                                "");
                        }
                        else
                        {
                            LogMessage(
                                $"Worker response missing 'status' property (attempt {attempt}/{WorkerReadyMaxAttempts}): {content}",
                                "");
                        }
                    }
                    catch (JsonException ex)
                    {
                        LogMessage(
                            $"Failed to parse worker status JSON (attempt {attempt}/{WorkerReadyMaxAttempts}): {ex.Message}",
                            "");
                    }
                }
                else
                {
                    LogMessage(
                        $"Worker not ready yet (attempt {attempt}/{WorkerReadyMaxAttempts}): HTTP {(int)response.StatusCode}",
                        "");
                }
            }
            catch (HttpRequestException ex)
            {
                var errorType = ex.InnerException?.GetType().Name ?? ex.GetType().Name;
                LogMessage(
                    $"Worker endpoint not available yet (attempt {attempt}/{WorkerReadyMaxAttempts}): {errorType}", "");
            }
            catch (TaskCanceledException ex) when (ex.InnerException is TimeoutException)
            {
                LogMessage($"Worker health check timeout (attempt {attempt}/{WorkerReadyMaxAttempts})", "");
            }
            catch (Exception ex)
            {
                if (attempt == WorkerReadyMaxAttempts)
                {
                    throw new TimeoutException(
                        $"Worker did not become ready after {WorkerReadyMaxAttempts} attempts", ex);
                }

                LogMessage(
                    $"Worker not ready yet (attempt {attempt}/{WorkerReadyMaxAttempts}): {ex.GetType().Name} - {ex.Message}",
                    "");
            }

            await Task.Delay(WorkerReadyDelayMs);
        }

        throw new TimeoutException(
            $"Worker and connectors did not reach 'Running' status after {WorkerReadyMaxAttempts} attempts");
    }
}
