using DotNet.Testcontainers.Networks;
using System.Text.Json;

namespace IntegrationTests.Kafka.Connect.Infrastructure.Fixtures;

public class DistributedFixture(
    TestConfiguration configuration,
    Action<string, string> logMessage,
    IContainerService containerService,
    INetwork network)
    : InfrastructureFixture(configuration, logMessage, containerService, network)
{
    private const int WorkerReadyMaxAttempts = 60;
    private const int WorkerReadyDelayMs = 1000;

    protected override string GetTargetName() => "distributed";

    public override async Task InitializeAsync()
    {
        LogMessage("Initializing Kafka Connect distributed worker...", "");
        
        await CreateContainersAsync();
        
        await Task.Delay(10000);
        
        var distributedConfig = Configuration.TestContainers.Containers.FirstOrDefault(c => c.Target == "distributed");
        if (distributedConfig?.WaitForHealthCheck == true)
        {
            await WaitForReadyAsync();
        }
        
        LogMessage("Kafka Connect distributed worker initialized!", "");
    }

    private async Task WaitForReadyAsync()
    {
        var distributedEndpoint = Configuration.GetServiceEndpoint("Distributed");
        var statusUrl = $"{distributedEndpoint}/workers/status";

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

                            // For distributed mode, we may not have connectors initially
                            // Just check if worker is running
                            if (workerRunning)
                            {
                                LogMessage(
                                    $"Distributed worker is ready (attempt {attempt}): Worker=Running",
                                    "");
                                return;
                            }

                            LogMessage(
                                $"Distributed worker not ready yet (attempt {attempt}/{WorkerReadyMaxAttempts}): Worker={workerRunning}",
                                "");
                        }
                        else
                        {
                            LogMessage(
                                $"Distributed worker response missing 'status' property (attempt {attempt}/{WorkerReadyMaxAttempts}): {content}",
                                "");
                        }
                    }
                    catch (JsonException ex)
                    {
                        LogMessage(
                            $"Failed to parse distributed worker status JSON (attempt {attempt}/{WorkerReadyMaxAttempts}): {ex.Message}",
                            "");
                    }
                }
                else
                {
                    LogMessage(
                        $"Distributed worker not ready yet (attempt {attempt}/{WorkerReadyMaxAttempts}): HTTP {(int)response.StatusCode}",
                        "");
                }
            }
            catch (HttpRequestException ex)
            {
                var errorType = ex.InnerException?.GetType().Name ?? ex.GetType().Name;
                LogMessage(
                    $"Distributed worker endpoint not available yet (attempt {attempt}/{WorkerReadyMaxAttempts}): {errorType}", "");
            }
            catch (TaskCanceledException ex) when (ex.InnerException is TimeoutException)
            {
                LogMessage($"Distributed worker health check timeout (attempt {attempt}/{WorkerReadyMaxAttempts})", "");
            }
            catch (Exception ex)
            {
                if (attempt == WorkerReadyMaxAttempts)
                {
                    throw new TimeoutException(
                        $"Distributed worker did not become ready after {WorkerReadyMaxAttempts} attempts", ex);
                }

                LogMessage(
                    $"Distributed worker not ready yet (attempt {attempt}/{WorkerReadyMaxAttempts}): {ex.GetType().Name} - {ex.Message}",
                    "");
            }

            await Task.Delay(WorkerReadyDelayMs);
        }

        throw new TimeoutException(
            $"Distributed worker did not reach 'Running' status after {WorkerReadyMaxAttempts} attempts");
    }
}
