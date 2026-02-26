using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;

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
