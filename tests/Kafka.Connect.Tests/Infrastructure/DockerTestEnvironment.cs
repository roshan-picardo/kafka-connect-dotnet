using Docker.DotNet;
using Docker.DotNet.Models;
using Microsoft.Extensions.Logging;
using System.Runtime.InteropServices;

namespace Kafka.Connect.Tests.Infrastructure;

public class DockerTestEnvironment : IAsyncDisposable
{
    private readonly ILogger<DockerTestEnvironment> _logger;
    private readonly DockerClient _dockerClient;
    private readonly string _composeProjectName;
    private readonly string _composeFilePath;
    private bool _isStarted;

    public DockerTestEnvironment(ILogger<DockerTestEnvironment> logger, string composeFilePath = "docker-compose.test.yml")
    {
        _logger = logger;
        _composeFilePath = composeFilePath;
        _composeProjectName = $"kafka-connect-tests-{Guid.NewGuid():N}";

        var dockerUri = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) 
            ? "npipe://./pipe/docker_engine" 
            : "unix:///var/run/docker.sock";

        _dockerClient = new DockerClientConfiguration(new Uri(dockerUri)).CreateClient();
    }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (_isStarted)
        {
            _logger.LogWarning("Docker test environment is already started");
            return;
        }

        _logger.LogInformation("Starting Docker test environment with project name: {ProjectName}", _composeProjectName);

        try
        {
            // Start services using docker-compose
            await ExecuteDockerComposeCommandAsync("up", "-d", "--build", "--force-recreate");

            // Wait for services to be healthy
            await WaitForServicesHealthyAsync(cancellationToken);

            _isStarted = true;
            _logger.LogInformation("Docker test environment started successfully");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to start Docker test environment");
            throw;
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (!_isStarted)
        {
            _logger.LogWarning("Docker test environment is not started");
            return;
        }

        _logger.LogInformation("Stopping Docker test environment: {ProjectName}", _composeProjectName);

        try
        {
            // Stop containers and remove volumes but preserve images
            await ExecuteDockerComposeCommandAsync("down", "-v", "--remove-orphans");
            
            // Only remove the custom Kafka Connect image
            await RemoveKafkaConnectImageAsync();
            
            _isStarted = false;
            _logger.LogInformation("Docker test environment stopped successfully (base images preserved)");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to stop Docker test environment");
            throw;
        }
    }

    private async Task RemoveKafkaConnectImageAsync()
    {
        try
        {
            _logger.LogInformation("Removing custom Kafka Connect image...");
            
            // Get list of images and find the Kafka Connect image built from our Dockerfile
            var images = await _dockerClient.Images.ListImagesAsync(new ImagesListParameters());
            
            var kafkaConnectImage = images.FirstOrDefault(img =>
                img.RepoTags?.Any(tag => tag.Contains(_composeProjectName) && tag.Contains("kafka-connect")) == true);
            
            if (kafkaConnectImage != null)
            {
                await _dockerClient.Images.DeleteImageAsync(kafkaConnectImage.ID, new ImageDeleteParameters
                {
                    Force = true
                });
                
                var imageTag = kafkaConnectImage.RepoTags?.FirstOrDefault() ?? kafkaConnectImage.ID;
                _logger.LogInformation("Removed Kafka Connect image: {ImageTag}", imageTag);
            }
            else
            {
                _logger.LogDebug("No custom Kafka Connect image found to remove");
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to remove Kafka Connect image (this is not critical)");
        }
    }

    public async Task RestartServiceAsync(string serviceName, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Restarting service: {ServiceName}", serviceName);

        try
        {
            await ExecuteDockerComposeCommandAsync("restart", serviceName);
            await WaitForServiceHealthyAsync(serviceName, cancellationToken);
            _logger.LogInformation("Service restarted successfully: {ServiceName}", serviceName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to restart service: {ServiceName}", serviceName);
            throw;
        }
    }

    public async Task<string> GetServiceLogsAsync(string serviceName, int tailLines = 100)
    {
        _logger.LogInformation("Getting logs for service: {ServiceName}", serviceName);

        try
        {
            var result = await ExecuteDockerComposeCommandAsync("logs", "--tail", tailLines.ToString(), serviceName);
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to get logs for service: {ServiceName}", serviceName);
            throw;
        }
    }

    private async Task<string> ExecuteDockerComposeCommandAsync(params string[] arguments)
    {
        var allArgs = new List<string>
        {
            "compose",
            "-f", _composeFilePath,
            "-p", _composeProjectName
        };
        allArgs.AddRange(arguments);

        var processStartInfo = new System.Diagnostics.ProcessStartInfo
        {
            FileName = "docker",
            Arguments = string.Join(" ", allArgs.Select(arg => arg.Contains(' ') ? $"\"{arg}\"" : arg)),
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };

        _logger.LogDebug("Executing: docker {Arguments}", processStartInfo.Arguments);

        using var process = System.Diagnostics.Process.Start(processStartInfo);
        if (process == null)
        {
            throw new InvalidOperationException("Failed to start docker process");
        }

        var output = await process.StandardOutput.ReadToEndAsync();
        var error = await process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();

        if (process.ExitCode != 0)
        {
            throw new InvalidOperationException($"Docker command failed with exit code {process.ExitCode}. Error: {error}");
        }

        return output;
    }

    private async Task WaitForServicesHealthyAsync(CancellationToken cancellationToken)
    {
        var services = new[]
        {
            "broker",
            "schema-registry",
            "mongodb",
            "postgres",
            "sqlserver",
            "mysql",
            "mariadb",
            "kafka-connect"
        };

        var tasks = services.Select(service => WaitForServiceHealthyAsync(service, cancellationToken));
        await Task.WhenAll(tasks);
    }

    private async Task WaitForServiceHealthyAsync(string serviceName, CancellationToken cancellationToken)
    {
        var timeout = TimeSpan.FromMinutes(5);
        var endTime = DateTime.UtcNow.Add(timeout);

        _logger.LogInformation("Waiting for service to be healthy: {ServiceName}", serviceName);

        while (DateTime.UtcNow < endTime && !cancellationToken.IsCancellationRequested)
        {
            try
            {
                var containers = await _dockerClient.Containers.ListContainersAsync(new ContainersListParameters
                {
                    Filters = new Dictionary<string, IDictionary<string, bool>>
                    {
                        ["label"] = new Dictionary<string, bool>
                        {
                            [$"com.docker.compose.project={_composeProjectName}"] = true,
                            [$"com.docker.compose.service={serviceName}"] = true
                        }
                    }
                }, cancellationToken);

                var container = containers.FirstOrDefault();
                if (container == null)
                {
                    _logger.LogDebug("Container not found for service: {ServiceName}", serviceName);
                    await Task.Delay(2000, cancellationToken);
                    continue;
                }

                // Check if container is running
                if (container.State != "running")
                {
                    _logger.LogDebug("Container not running for service: {ServiceName}, state: {State}", serviceName, container.State);
                    await Task.Delay(2000, cancellationToken);
                    continue;
                }

                // For services with health checks, wait for healthy status
                if (container.Status.Contains("healthy") || !container.Status.Contains("health"))
                {
                    _logger.LogInformation("Service is healthy: {ServiceName}", serviceName);
                    return;
                }

                if (container.Status.Contains("unhealthy"))
                {
                    _logger.LogWarning("Service is unhealthy: {ServiceName}, status: {Status}", serviceName, container.Status);
                }

                await Task.Delay(2000, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Error checking service health: {ServiceName}", serviceName);
                await Task.Delay(2000, cancellationToken);
            }
        }

        if (cancellationToken.IsCancellationRequested)
        {
            throw new OperationCanceledException("Service health check was cancelled");
        }

        throw new TimeoutException($"Service {serviceName} did not become healthy within {timeout}");
    }

    public async Task<bool> IsServiceHealthyAsync(string serviceName)
    {
        try
        {
            var containers = await _dockerClient.Containers.ListContainersAsync(new ContainersListParameters
            {
                Filters = new Dictionary<string, IDictionary<string, bool>>
                {
                    ["label"] = new Dictionary<string, bool>
                    {
                        [$"com.docker.compose.project={_composeProjectName}"] = true,
                        [$"com.docker.compose.service={serviceName}"] = true
                    }
                }
            });

            var container = containers.FirstOrDefault();
            return container?.State == "running" && 
                   (container.Status.Contains("healthy") || !container.Status.Contains("health"));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error checking if service is healthy: {ServiceName}", serviceName);
            return false;
        }
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (_isStarted)
            {
                await StopAsync();
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error disposing Docker test environment");
        }
        finally
        {
            _dockerClient?.Dispose();
        }
    }
}