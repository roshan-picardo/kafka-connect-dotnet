using Confluent.Kafka;
using Confluent.Kafka.Admin;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using System.Net.Sockets;
using System.Text.Json;

namespace IntegrationTests.Kafka.Connect.Infrastructure.Fixtures;

public class KafkaFixture(
    TestConfiguration configuration,
    Action<string, string> logMessage,
    IContainerService containerService,
    INetwork network)
    : InfrastructureFixture(configuration, logMessage, containerService, network)
{
    private IAdminClient? _adminClient;
    private readonly List<IContainer> _containers = new();

    protected override string GetTargetName() => "kafka";

    public override async Task InitializeAsync()
    {
        await CreateKafkaContainersAsync();
        await CreateConnectorTopicsAsync();
    }

    private async Task CreateKafkaContainersAsync()
    {
        var targetName = GetTargetName();
        var allContainers = Configuration.TestContainers.Containers;

        var targetContainers = allContainers
            .Where(c => c.Target?.Equals(targetName, StringComparison.OrdinalIgnoreCase) == true && c.Enabled)
            .ToList();

        // Step 1: Start Zookeeper first
        var zookeeperConfig = targetContainers.FirstOrDefault(c =>
            c.Name.Contains("zookeeper", StringComparison.OrdinalIgnoreCase));
        
        if (zookeeperConfig != null)
        {
            var zookeeperContainer = await ContainerService.CreateContainerAsync(
                zookeeperConfig, Network, new TestLoggingService());
            _containers.Add(zookeeperContainer);
            
            // Wait for Zookeeper to be ready
            await WaitForZookeeperAsync();
        }

        // Step 2: Start Broker and wait for it to be ready
        var brokerConfig = targetContainers.FirstOrDefault(c =>
            c.Name.Contains("broker", StringComparison.OrdinalIgnoreCase) ||
            (c.Name.Contains("kafka", StringComparison.OrdinalIgnoreCase) &&
             !c.Name.Contains("zookeeper", StringComparison.OrdinalIgnoreCase)));
        
        if (brokerConfig != null)
        {
            var brokerContainer = await ContainerService.CreateContainerAsync(
                brokerConfig, Network, new TestLoggingService());
            _containers.Add(brokerContainer);
            
            // Wait for Broker to be ready before starting Schema Registry
            await WaitForBrokerAsync();
        }

        // Step 3: Start Schema Registry after Broker is ready
        var schemaConfig = targetContainers.FirstOrDefault(c =>
            c.Name.Contains("schema", StringComparison.OrdinalIgnoreCase));

        if (schemaConfig != null)
        {
            var schemaContainer = await ContainerService.CreateContainerAsync(
                schemaConfig, Network, new TestLoggingService());
            _containers.Add(schemaContainer);
            
            await WaitForSchemaRegistryAsync();
        }
        
        LogMessage($"Infrastructure is ready: {GetTargetName()}", "");
    }

    private async Task WaitForZookeeperAsync()
    {
        var zookeeperEndpoint = Configuration.GetServiceEndpoint("Zookeeper");
        if (string.IsNullOrEmpty(zookeeperEndpoint))
        {
            throw new InvalidOperationException("Zookeeper endpoint is not configured");
        }
        
        var parts = zookeeperEndpoint.Split(':');
        var host = parts[0];
        var port = int.Parse(parts[1]);

        for (var attempt = 1; attempt <= ReadyMaxAttempts; attempt++)
        {
            try
            {
                using var client = new TcpClient();
                await client.ConnectAsync(host, port);
                
                var stream = client.GetStream();
                var command = System.Text.Encoding.ASCII.GetBytes("ruok");
                await stream.WriteAsync(command);
                
                var buffer = new byte[4];
                var bytesRead = await stream.ReadAsync(buffer);
                var response = System.Text.Encoding.ASCII.GetString(buffer, 0, bytesRead);
                
                if (response == "imok")
                {
                    LogMessage("Started: zookeeper", "");
                    return;
                }
            }
            catch (Exception)
            {
                if (attempt == ReadyMaxAttempts)
                {
                    throw new TimeoutException(
                        $"Failed to start zookeeper after {ReadyMaxAttempts} attempts");
                }

                LogMessage($"Starting: zookeeper (attempt: {attempt}/{ReadyMaxAttempts})", "");
                await Task.Delay(ReadyDelayMs);
            }
        }
    }

    private async Task WaitForBrokerAsync()
    {
        var bootstrapServers = Configuration.GetServiceEndpoint("Kafka");
        if (string.IsNullOrEmpty(bootstrapServers))
        {
            throw new InvalidOperationException("Kafka endpoint is not configured");
        }

        for (var attempt = 1; attempt <= ReadyMaxAttempts; attempt++)
        {
            try
            {
                using var adminClient = new AdminClientBuilder(new AdminClientConfig
                    {
                        BootstrapServers = bootstrapServers,
                        SocketTimeoutMs = 5000,

                    }).SetLogHandler((_, _) =>
                    {
                    })
                    .SetErrorHandler((_, _) =>
                    {
                    })
                    .Build();

                var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));
                
                if (metadata.Brokers.Count > 0)
                {
                    LogMessage("Started: broker", "");
                    return;
                }
            }
            catch (Exception)
            {
                if (attempt == ReadyMaxAttempts)
                {
                    throw new TimeoutException(
                        $"Failed to start broker after {ReadyMaxAttempts} attempts");
                }

                LogMessage($"Starting: broker (attempt: {attempt}/{ReadyMaxAttempts})", "");
                await Task.Delay(ReadyDelayMs);
            }
        }
    }

    private async Task WaitForSchemaRegistryAsync()
    {
        var schemaRegistryUrl = Configuration.GetServiceEndpoint("SchemaRegistry");
        if (string.IsNullOrEmpty(schemaRegistryUrl))
        {
            throw new InvalidOperationException("SchemaRegistry endpoint is not configured");
        }
        
        using var httpClient = new HttpClient
        {
            Timeout = TimeSpan.FromSeconds(5)
        };

        for (var attempt = 1; attempt <= ReadyMaxAttempts; attempt++)
        {
            try
            {
                var response = await httpClient.GetAsync($"{schemaRegistryUrl}/subjects");
                
                if (response.IsSuccessStatusCode)
                {
                    LogMessage("Started: schema-registry", "");
                    return;
                }
            }
            catch (Exception)
            {
                if (attempt == ReadyMaxAttempts)
                {
                    throw new TimeoutException(
                        $"Failed to start schema-registry after {ReadyMaxAttempts} attempts");
                }

                LogMessage($"Starting: schema-registry (attempt: {attempt}/{ReadyMaxAttempts})", "");
                await Task.Delay(ReadyDelayMs);
            }
        }
    }

    private async Task CreateConnectorTopicsAsync()
    {
        LogMessage("Creating topics from connector configurations...", "");

        var configDirectory = Path.Join(Directory.GetCurrentDirectory(), "Configurations");
        var configFiles = new List<string>();

        var baseConfigFile = Path.Join(configDirectory, "appsettings.json");
        if (File.Exists(baseConfigFile))
        {
            configFiles.Add(baseConfigFile);
        }

        var patternConfigFiles = Directory.GetFiles(configDirectory, "appsettings.*.json");
        configFiles.AddRange(patternConfigFiles);
        
        // Also search in standalone subdirectory
        var standaloneDirectory = Path.Join(configDirectory, "standalone");
        if (Directory.Exists(standaloneDirectory))
        {
            var standaloneConfigFiles = Directory.GetFiles(standaloneDirectory, "appsettings.*.json");
            configFiles.AddRange(standaloneConfigFiles);
        }
        
        // Also search in distributed subdirectory
        var distributedDirectory = Path.Join(configDirectory, "distributed");
        if (Directory.Exists(distributedDirectory))
        {
            var distributedConfigFiles = Directory.GetFiles(distributedDirectory, "*.json");
            configFiles.AddRange(distributedConfigFiles);
        }

        if (configFiles.Count == 0)
        {
            LogMessage("No connector configuration files found, skipping topic creation", "");
            return;
        }

        var allTopics = new HashSet<string>();

        foreach (var configFile in configFiles)
        {
            try
            {
                var configContent = await File.ReadAllTextAsync(configFile);
                var configJson = JsonDocument.Parse(configContent);

                // Handle standalone format: worker.connectors
                if (configJson.RootElement.TryGetProperty("worker", out var worker))
                {
                    if (worker.TryGetProperty("topics", out var workerTopics))
                    {
                        foreach (var workerTopic in workerTopics.EnumerateObject())
                        {
                            var topicName = workerTopic.Value.GetString();
                            if (!string.IsNullOrEmpty(topicName))
                            {
                                var partitions = workerTopic.Name.Equals("config", StringComparison.OrdinalIgnoreCase) ? 1 : 50;
                                try
                                {
                                    await CreateTopicAsync(topicName, partitions: partitions);
                                    LogMessage($"Created {workerTopic.Name} topic: {topicName} ({partitions} partition{(partitions > 1 ? "s" : "")})", "");
                                }
                                catch (Exception ex)
                                {
                                    LogMessage($"Failed to create {workerTopic.Name} topic {topicName}: {ex.Message}", "");
                                }
                            }
                        }
                    }

                    // Handle connector-level topics
                    if (worker.TryGetProperty("connectors", out var connectors))
                    {
                        foreach (var connector in connectors.EnumerateObject())
                        {
                            // Handle sink connector topics (from topics array)
                            if (connector.Value.TryGetProperty("topics", out var topics))
                            {
                                foreach (var topic in topics.EnumerateArray())
                                {
                                    var topicName = topic.GetString();
                                    if (!string.IsNullOrEmpty(topicName))
                                    {
                                        allTopics.Add(topicName);
                                    }
                                }
                            }

                            if (connector.Value.TryGetProperty("plugin", out var plugin) &&
                                plugin.TryGetProperty("properties", out var properties) &&
                                properties.TryGetProperty("commands", out var commands))
                            {
                                foreach (var command in commands.EnumerateObject())
                                {
                                    if (command.Value.TryGetProperty("topic", out var commandTopic))
                                    {
                                        var topicName = commandTopic.GetString();
                                        if (!string.IsNullOrEmpty(topicName))
                                        {
                                            allTopics.Add(topicName);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                // Handle distributed format: connector at root level
                else if (configJson.RootElement.TryGetProperty("connector", out var connector))
                {
                    // Handle sink connector topics (from topics array)
                    if (connector.TryGetProperty("topics", out var topics))
                    {
                        foreach (var topic in topics.EnumerateArray())
                        {
                            var topicName = topic.GetString();
                            if (!string.IsNullOrEmpty(topicName))
                            {
                                allTopics.Add(topicName);
                            }
                        }
                    }

                    // Handle source connector topics (from plugin.properties.commands)
                    if (connector.TryGetProperty("plugin", out var plugin) &&
                        plugin.TryGetProperty("properties", out var properties) &&
                        properties.TryGetProperty("commands", out var commands))
                    {
                        foreach (var command in commands.EnumerateObject())
                        {
                            if (command.Value.TryGetProperty("topic", out var commandTopic))
                            {
                                var topicName = commandTopic.GetString();
                                if (!string.IsNullOrEmpty(topicName))
                                {
                                    allTopics.Add(topicName);
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogMessage($"Failed to parse config file {Path.GetFileName(configFile)}: {ex.Message}", "");
            }
        }

        foreach (var topic in allTopics)
        {
            try
            {
                await CreateTopicAsync(topic);
                LogMessage($"Created topic: {topic}", "");
            }
            catch (Exception ex)
            {
                LogMessage($"Failed to create topic {topic}: {ex.Message}", "");
            }
        }

        LogMessage($"Topic creation completed. Created {allTopics.Count} topics.", "");
    }

    private async Task CreateTopicAsync(string topicName, int partitions = 1, short replicationFactor = 1)
    {
        var bootstrapServers = Configuration.GetServiceEndpoint("Kafka");
        if (string.IsNullOrEmpty(bootstrapServers))
        {
            throw new InvalidOperationException("Kafka endpoint is not configured");
        }

        _adminClient ??= new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = bootstrapServers
            })
            .SetLogHandler((_, logMessage) =>
            {
                if (Configuration.DetailedLog)
                {
                    LogMessage(logMessage.Message, "");
                }
            })
            .SetErrorHandler((_, error) =>
            {
                if (Configuration.DetailedLog)
                {
                    LogMessage($"Kafka Admin Client Error: {error.Reason}", "");
                }
            })
            .Build();

        var topicSpecification = new TopicSpecification
        {
            Name = topicName,
            NumPartitions = partitions,
            ReplicationFactor = replicationFactor
        };

        try
        {
            await _adminClient.CreateTopicsAsync([topicSpecification], new CreateTopicsOptions
            {
                RequestTimeout = TimeSpan.FromSeconds(30)
            });
        }
        catch (CreateTopicsException ex)
        {
            if (ex.Results[0].Error.Code != ErrorCode.TopicAlreadyExists)
            {
                throw;
            }
        }
    }

    public override async ValueTask DisposeAsync()
    {
        _adminClient?.Dispose();
        
        // Dispose all containers in parallel
        await Task.WhenAll(_containers.Select(c => c.DisposeAsync().AsTask()));
        
        await base.DisposeAsync();
    }
}
