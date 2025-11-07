using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;

namespace IntegrationTests.Kafka.Connect.Infrastructure;

public interface IContainerService
{
    Task<IContainer> CreateContainerAsync<T>(T config, INetwork network, TestLoggingService loggingService) where T : ContainerConfig;
    Task<IContainer> CreateKafkaConnectContainerAsync(KafkaConnectConfig config, INetwork network, TestLoggingService loggingService);
}

public class ContainerService : IContainerService
{
    public async Task<IContainer> CreateContainerAsync<T>(T config, INetwork network, TestLoggingService loggingService) where T : ContainerConfig
    {
        var containerBuilder = new ContainerBuilder()
            .WithImage(config.Image)
            .WithNetwork(network)
            .WithHostname(config.Hostname)
            .WithName(config.Name);

        containerBuilder = config.NetworkAliases.Aggregate(containerBuilder, (current, alias) => current.WithNetworkAliases(alias));

        containerBuilder = config.Environment.Aggregate(containerBuilder, (current, env) => current.WithEnvironment(env.Key, env.Value));

        if (config.Ports.Count > 0)
        {
            foreach (var portMapping in config.Ports)
            {
                var (hostPort, containerPort) = ParsePortMapping(portMapping);
                containerBuilder = containerBuilder.WithPortBinding(hostPort, containerPort);
            }
        }

        var container = containerBuilder.Build();
        TestLoggingService.LogMessage($"Creating container: {config.Name} ({config.Image})");
        await container.StartAsync();
        TestLoggingService.LogMessage($"Container started: {config.Name}");
        return container;
    }

    public async Task<IContainer> CreateKafkaConnectContainerAsync(KafkaConnectConfig config, INetwork network, TestLoggingService loggingService)
    {
        var currentDir = Directory.GetCurrentDirectory();
        var projectRoot = currentDir;
        while (!File.Exists(Path.Combine(projectRoot, config.DockerfilePath)))
        {
            var parent = Directory.GetParent(projectRoot);
            if (parent == null) break;
            projectRoot = parent.FullName;
        }

        if (!File.Exists(Path.Combine(projectRoot, config.DockerfilePath)))
        {
            throw new FileNotFoundException($"Could not find {config.DockerfilePath} in project hierarchy");
        }

        var dockerContextPath = projectRoot;

        var futureImage = new ImageFromDockerfileBuilder()
            .WithDockerfileDirectory(new CommonDirectoryPath(dockerContextPath), string.Empty)
            .WithDockerfile(config.DockerfilePath)
            .WithName("kafka-connect:latest")
            .WithCleanUp(config.CleanUpImage)
            .Build();

        await futureImage.CreateAsync();

        var testProjectDir = Path.GetDirectoryName(typeof(TestFixture).Assembly.Location);
        var configurationsPath = Path.Combine(testProjectDir!, "..", "..", "..", config.ConfigurationsPath);
        configurationsPath = Path.GetFullPath(configurationsPath);

        var containerBuilder = new ContainerBuilder()
            .WithImage(futureImage)
            .WithNetwork(network)
            .WithHostname(config.Hostname)
            .WithName(config.Name)
            .WithOutputConsumer(Consume.RedirectStdoutAndStderrToStream(
                new KafkaConnectLogBuffer(),
                new KafkaConnectLogBuffer()));

        containerBuilder = config.NetworkAliases.Aggregate(containerBuilder, (current, alias) => current.WithNetworkAliases(alias));

        foreach (var bindMount in config.BindMounts)
        {
            var sourcePath = bindMount.Key == "Configurations" ? configurationsPath : bindMount.Key;
            containerBuilder = containerBuilder.WithBindMount(sourcePath, bindMount.Value);
        }

        if (config.Command.Count > 0)
        {
            containerBuilder = containerBuilder.WithCommand(config.Command.ToArray());
        }

        if (config.Ports.Count > 0)
        {
            foreach (var portMapping in config.Ports)
            {
                var (hostPort, containerPort) = ParsePortMapping(portMapping);
                containerBuilder = containerBuilder.WithPortBinding(hostPort, containerPort);
            }
        }

        var container = containerBuilder.Build();

        TestLoggingService.LogMessage($"Creating Kafka Connect Worker: {config.Name}");
        await container.StartAsync();
        TestLoggingService.LogMessage($"Kafka Connect Worker started: {config.Name}");

        return container;
    }

    private (int hostPort, int containerPort) ParsePortMapping(string portMapping)
    {
        if (portMapping.Contains(':'))
        {
            var parts = portMapping.Split(':');
            if (parts.Length == 2 && int.TryParse(parts[0], out var host) && int.TryParse(parts[1], out var container))
            {
                return (host, container);
            }
        }
        else if (int.TryParse(portMapping, out var port))
        {
            return (port, port);
        }
            
        throw new ArgumentException($"Invalid port mapping format: {portMapping}. Use 'port' or 'hostPort:containerPort'");
    }
}