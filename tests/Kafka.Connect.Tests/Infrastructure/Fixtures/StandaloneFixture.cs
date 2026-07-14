using DotNet.Testcontainers.Networks;

namespace IntegrationTests.Kafka.Connect.Infrastructure.Fixtures;

public class StandaloneFixture(
    TestConfiguration configuration,
    Action<string, string> logMessage,
    IContainerService containerService,
    INetwork network)
    : InfrastructureFixture(configuration, logMessage, containerService, network)
{
    protected override string GetTargetName() => "standalone";

    public override async Task InitializeAsync()
    {
        FilterStandaloneCommands();

        await CreateContainersAsync();
        
        await Task.Delay(ReadyDelayMs);
        
        var standaloneConfig = Configuration.TestContainers.Containers.FirstOrDefault(c => c.Target == "standalone");
        if (standaloneConfig?.WaitForHealthCheck == true)
        {
            var standaloneEndpoint = Configuration.GetServiceEndpoint("Standalone");
            var statusUrl = $"{standaloneEndpoint}/workers/status";
            await WaitForWorkerReadyAsync(statusUrl, $"{GetTargetName()} worker");
        }
        
        LogMessage($"Kafka Connect {GetTargetName()} worker is ready.", "");
    }

    private void FilterStandaloneCommands()
    {
        if (Configuration.Targets.Count == 0) return;

        var activeTargets = Configuration.Targets.Select(t => t.ToLowerInvariant()).ToHashSet();

        var standaloneContainer = Configuration.TestContainers.Containers
            .FirstOrDefault(c => c.Target == "standalone");

        if (standaloneContainer == null) return;

        var removed = standaloneContainer.Command.RemoveAll(arg =>
        {
            // Match --config=.../standalone/appsettings.{target}.json
            var match = System.Text.RegularExpressions.Regex.Match(
                arg, @"appsettings\.([^./]+)\.json$");
            if (!match.Success) return false;
            var target = match.Groups[1].Value.ToLowerInvariant();
            // Keep non-plugin configs (appsettings.json, appsettings.worker.json, appsettings.standalone.json)
            var pluginTargets = new HashSet<string> { "mongodb", "postgres", "sqlserver", "mysql", "mariadb", "oracle", "dynamodb", "cassandra" };
            return pluginTargets.Contains(target) && !activeTargets.Contains(target);
        });

        if (removed > 0)
            LogMessage($"Standalone: removed {removed} inactive plugin config(s) from command", "");
    }
}
