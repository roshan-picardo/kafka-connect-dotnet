using DotNet.Testcontainers.Networks;
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
}
