using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using System.Text.Json;

namespace IntegrationTests.Kafka.Connect.Infrastructure.Fixtures;

public abstract class InfrastructureFixture(
    TestConfiguration configuration,
    Action<string, string> logMessage,
    IContainerService containerService,
    INetwork network)
    : IAsyncDisposable
{
    protected readonly TestConfiguration Configuration = configuration;
    protected readonly Action<string, string> LogMessage = logMessage;
    private readonly List<IContainer> _containers = new();
    
    protected const int WorkerReadyMaxAttempts = 60;
    protected const int WorkerReadyDelayMs = 1000;

    public abstract Task InitializeAsync();

    protected abstract string GetTargetName();

    protected async Task CreateContainersAsync()
    {
        var targetName = GetTargetName();
        var allContainers = Configuration.TestContainers.Containers;

        var targetContainers = allContainers
            .Where(c => c.Target?.Equals(targetName, StringComparison.OrdinalIgnoreCase) == true && c.Enabled)
            .ToList();

        foreach (var config in targetContainers)
        {
            var container = await containerService.CreateContainerAsync(config, network, new TestLoggingService());
            _containers.Add(container);
        }
    }

    /// <summary>
    /// Common method to wait for a worker to be ready with retry logic
    /// </summary>
    public async Task WaitForWorkerReadyAsync(string statusUrl, string workerName, Func<List<string>, Task>? retryFailedConnectorsCallback = null, bool silent = false)
    {
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

                            var allConnectorsRunning = true;
                            var connectorStatuses = new List<string>();
                            var failedConnectors = new List<string>();

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
                                            failedConnectors.Add(connectorName!);
                                        }
                                    }
                                }
                            }

                            if (workerRunning && allConnectorsRunning && connectorStatuses.Count > 0)
                            {
                                if (!silent)
                                {
                                    LogMessage(
                                        $"{workerName} and connectors are ready (attempt {attempt}): Worker=Running, Connectors=[{string.Join(", ", connectorStatuses)}]",
                                        "");
                                }
                                return;
                            }

                            // If connectors are not running and callback is provided, retry submitting them
                            if (workerRunning && !allConnectorsRunning && failedConnectors.Count > 0 && retryFailedConnectorsCallback != null)
                            {
                                LogMessage(
                                    $"{workerName} has failed connectors (attempt {attempt}/{WorkerReadyMaxAttempts}): [{string.Join(", ", failedConnectors)}]. Retrying submission...",
                                    "");
                                
                                await retryFailedConnectorsCallback(failedConnectors);
                                
                                // Give some time for the resubmission to take effect
                                await Task.Delay(2000);
                            }
                            else
                            {
                                if (!silent)
                                {
                                    LogMessage(
                                        $"{workerName} or connectors not ready yet (attempt {attempt}/{WorkerReadyMaxAttempts}): Worker={workerRunning}, Connectors=[{string.Join(", ", connectorStatuses)}]",
                                        "");
                                }
                            }
                        }
                        else
                        {
                            LogMessage(
                                $"{workerName} response missing 'status' property (attempt {attempt}/{WorkerReadyMaxAttempts}): {content}",
                                "");
                        }
                    }
                    catch (JsonException ex)
                    {
                        LogMessage(
                            $"Failed to parse {workerName} status JSON (attempt {attempt}/{WorkerReadyMaxAttempts}): {ex.Message}",
                            "");
                    }
                }
                else
                {
                    if (!silent)
                    {
                        LogMessage(
                            $"{workerName} not ready yet (attempt {attempt}/{WorkerReadyMaxAttempts}): HTTP {(int)response.StatusCode}",
                            "");
                    }
                }
            }
            catch (HttpRequestException ex)
            {
                if (!silent)
                {
                    var errorType = ex.InnerException?.GetType().Name ?? ex.GetType().Name;
                    LogMessage(
                        $"{workerName} endpoint not available yet (attempt {attempt}/{WorkerReadyMaxAttempts}): {errorType}", "");
                }
            }
            catch (TaskCanceledException ex) when (ex.InnerException is TimeoutException)
            {
                if (!silent)
                {
                    LogMessage($"{workerName} health check timeout (attempt {attempt}/{WorkerReadyMaxAttempts})", "");
                }
            }
            catch (Exception ex)
            {
                if (attempt == WorkerReadyMaxAttempts)
                {
                    throw new TimeoutException(
                        $"{workerName} did not become ready after {WorkerReadyMaxAttempts} attempts", ex);
                }

                if (!silent)
                {
                    LogMessage(
                        $"{workerName} not ready yet (attempt {attempt}/{WorkerReadyMaxAttempts}): {ex.GetType().Name} - {ex.Message}",
                        "");
                }
            }

            await Task.Delay(WorkerReadyDelayMs);
        }

        throw new TimeoutException(
            $"{workerName} and connectors did not reach 'Running' status after {WorkerReadyMaxAttempts} attempts");
    }

    public virtual async ValueTask DisposeAsync()
    {
        foreach (var container in _containers.AsEnumerable().Reverse())
        {
            try
            {
                LogMessage($"Stopping container: {container.Name}", "");
                await container.StopAsync();
                await container.DisposeAsync();
            }
            catch (Exception ex)
            {
                LogMessage($"Error disposing container {container.Name}: {ex.Message}", "");
            }
        }

        _containers.Clear();
        GC.SuppressFinalize(this);
    }
}
