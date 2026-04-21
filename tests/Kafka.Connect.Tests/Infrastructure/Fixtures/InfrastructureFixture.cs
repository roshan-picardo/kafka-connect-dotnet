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
    protected readonly IContainerService ContainerService = containerService;
    protected readonly INetwork Network = network;
    private readonly List<IContainer> _containers = new();

    // Readiness check configuration
    protected const int ReadyMaxAttempts = 60;
    protected const int ReadyDelayMs = 1000;

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
            var container = await ContainerService.CreateContainerAsync(config, Network, new TestLoggingService());
            _containers.Add(container);
        }
    }

    public async Task WaitForWorkerReadyAsync(string statusUrl, string workerName, Func<List<string>, Task>? retryFailedConnectorsCallback = null, bool silent = false)
    {
        using var httpClient = new HttpClient();
        httpClient.Timeout = TimeSpan.FromSeconds(15);

        for (var attempt = 1; attempt <= ReadyMaxAttempts; attempt++)
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
                                        $"Started: {workerName} and connectors.", "");
                                }
                                return;
                            }

                            // If connectors are not running and callback is provided, retry submitting them
                            if (workerRunning && !allConnectorsRunning && failedConnectors.Count > 0 && retryFailedConnectorsCallback != null)
                            {
                                LogMessage(
                                    $"Starting: {workerName} (attempt {attempt}/{ReadyMaxAttempts})", "");
                                
                                await retryFailedConnectorsCallback(failedConnectors);
                                
                                await Task.Delay(ReadyDelayMs);
                            }
                            else
                            {
                                if (!silent)
                                {
                                    LogMessage(
                                        $"Staring: {workerName} (attempt: {attempt}/{ReadyMaxAttempts})", "");
                                }
                            }
                        }
                        else
                        {
                            LogMessage(
                                $"Failed to start {workerName} after (attempt: {attempt}/{ReadyMaxAttempts}) : Response missing 'status' property: {content} ", "");
                        }
                    }
                    catch (JsonException ex)
                    {
                        LogMessage(
                            $"Failed start {workerName} after (attempt: {attempt}/{ReadyMaxAttempts}): {ex.Message}",
                            "");
                    }
                }
                else
                {
                    if (!silent)
                    {
                        LogMessage(
                            $"Starting: {workerName} (attempt {attempt}/{ReadyMaxAttempts})",
                            "");
                    }
                }
            }
            catch (HttpRequestException)
            {
                if (!silent)
                {
                    LogMessage($"Staring: {workerName} (attempt {attempt}/{ReadyMaxAttempts})", "");
                }
            }
            catch (TaskCanceledException ex) when (ex.InnerException is TimeoutException)
            {
                if (!silent)
                {
                    LogMessage($"Starting: {workerName} (attempt {attempt}/{ReadyMaxAttempts})", $"Health check timeout: {ex.InnerException.Message}");
                }
            }
            catch (Exception ex)
            {
                if (attempt == ReadyMaxAttempts)
                {
                    throw new TimeoutException(
                        $"Failed to start {workerName} after {ReadyMaxAttempts} attempts", ex);
                }

                if (!silent)
                {
                    LogMessage(
                        $"Failed to start {workerName} (attempt {attempt}/{ReadyMaxAttempts}): {ex.GetType().Name} - {ex.Message}",
                        "");
                }
            }

            await Task.Delay(ReadyDelayMs);
        }

        throw new TimeoutException(
            $"Failed to start {workerName} after {ReadyMaxAttempts} attempts");
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
