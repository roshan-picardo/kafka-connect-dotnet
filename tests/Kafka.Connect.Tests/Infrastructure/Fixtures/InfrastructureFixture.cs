using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;

namespace IntegrationTests.Kafka.Connect.Infrastructure.Fixtures;

public abstract class InfrastructureFixture : IAsyncDisposable
{
    protected readonly TestConfiguration Configuration;
    protected readonly Action<string, string> LogMessage;
    protected readonly IContainerService ContainerService;
    protected readonly INetwork Network;
    protected readonly List<IContainer> Containers = new();

    protected InfrastructureFixture(
        TestConfiguration configuration,
        Action<string, string> logMessage,
        IContainerService containerService,
        INetwork network)
    {
        Configuration = configuration;
        LogMessage = logMessage;
        ContainerService = containerService;
        Network = network;
    }

    /// <summary>
    /// Initialize the infrastructure component (create containers, wait for readiness, etc.)
    /// </summary>
    public abstract Task InitializeAsync();

    /// <summary>
    /// Get the target name for this infrastructure component (e.g., "kafka", "postgres", "mongodb")
    /// </summary>
    protected abstract string GetTargetName();

    /// <summary>
    /// Create containers for this infrastructure component based on Target property
    /// </summary>
    protected async Task CreateContainersAsync()
    {
        var targetName = GetTargetName();
        var allContainers = Configuration.TestContainers.Containers;

        // Find containers that match this target
        var targetContainers = allContainers
            .Where(c => c.Target?.Equals(targetName, StringComparison.OrdinalIgnoreCase) == true && c.Enabled)
            .ToList();

        foreach (var config in targetContainers)
        {
            var container = await ContainerService.CreateContainerAsync(config, Network, new TestLoggingService());
            Containers.Add(container);
        }
    }

    public virtual async ValueTask DisposeAsync()
    {
        foreach (var container in Containers.AsEnumerable().Reverse())
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

        Containers.Clear();
        GC.SuppressFinalize(this);
    }
}
