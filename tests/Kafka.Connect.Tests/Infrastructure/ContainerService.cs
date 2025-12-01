using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;

namespace IntegrationTests.Kafka.Connect.Infrastructure;

public interface IContainerService
{
    Task<IContainer> CreateContainerAsync(ContainerConfig config, INetwork network, TestLoggingService loggingService);
}

public class ContainerService : IContainerService
{
    public async Task<IContainer> CreateContainerAsync(ContainerConfig config, INetwork network, TestLoggingService loggingService)
    {
        ContainerBuilder containerBuilder;

        if (!string.IsNullOrEmpty(config.DockerfilePath))
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

            var futureImage = new ImageFromDockerfileBuilder()
                .WithDockerfileDirectory(new CommonDirectoryPath(projectRoot), string.Empty)
                .WithDockerfile(config.DockerfilePath)
                .WithName($"{config.Name}:latest")
                .WithCleanUp(config.CleanUpImage)
                .Build();

            await futureImage.CreateAsync();

            containerBuilder = new ContainerBuilder()
                .WithImage(futureImage)
                .WithOutputConsumer(Consume.RedirectStdoutAndStderrToStream(
                    new KafkaConnectLogBuffer(),
                    new KafkaConnectLogBuffer()));
        }
        else
        {
            containerBuilder = new ContainerBuilder()
                .WithImage(config.Image);
        }

        containerBuilder = containerBuilder
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

        if (config.BindMounts.Count > 0)
        {
            var testProjectDir = Path.GetDirectoryName(typeof(TestFixture).Assembly.Location);
            foreach (var bindMount in config.BindMounts)
            {
                var sourcePath = bindMount.Key == "Configurations"
                    ? Path.GetFullPath(Path.Combine(testProjectDir!, "..", "..", "..", config.ConfigurationsPath ?? "Configurations"))
                    : bindMount.Key;
                containerBuilder = containerBuilder.WithBindMount(sourcePath, bindMount.Value);
            }
        }

        if (config.Command.Count > 0)
        {
            containerBuilder = containerBuilder.WithCommand(config.Command.ToArray());
        }

        var container = containerBuilder.Build();
        
        TestLoggingService.LogMessage($"Creating container: {config.Name} ({config.Image})");
        await container.StartAsync();
        TestLoggingService.LogMessage($"Container started: {config.Name}");
        
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