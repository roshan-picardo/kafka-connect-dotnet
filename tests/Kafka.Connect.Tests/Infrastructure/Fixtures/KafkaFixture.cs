using Confluent.Kafka;
using Confluent.Kafka.Admin;
using DotNet.Testcontainers.Networks;
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

    protected override string GetTargetName() => "kafka";

    public override async Task InitializeAsync()
    {
        await CreateContainersAsync();
        await Task.Delay(5000);
        await CreateConnectorTopicsAsync();
        LogMessage("Kafka infrastructure initialized!", "");
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

        if (configFiles.Count == 0)
        {
            LogMessage("No connector configuration files found, skipping topic creation", "");
            return;
        }

        var allTopics = new HashSet<string>();
        var systemTopics = new HashSet<string>();

        foreach (var configFile in configFiles)
        {
            try
            {
                var configContent = await File.ReadAllTextAsync(configFile);
                var configJson = JsonDocument.Parse(configContent);

                if (configJson.RootElement.TryGetProperty("worker", out var worker))
                {
                    if (worker.TryGetProperty("topics", out var workerTopics))
                    {
                        foreach (var workerTopic in workerTopics.EnumerateObject())
                        {
                            var topicName = workerTopic.Name;
                            if (!string.IsNullOrEmpty(topicName))
                            {
                                systemTopics.Add(topicName);
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
            }
            catch (Exception ex)
            {
                LogMessage($"Failed to parse config file {Path.GetFileName(configFile)}: {ex.Message}", "");
            }
        }

        foreach (var topic in systemTopics)
        {
            try
            {
                await CreateTopicAsync(topic, partitions: 50);
                LogMessage($"Created system topic: {topic} (50 partitions)", "");
            }
            catch (Exception ex)
            {
                LogMessage($"Failed to create system topic {topic}: {ex.Message}", "");
            }
        }

        foreach (var topic in allTopics)
        {
            try
            {
                await CreateTopicAsync(topic);
                LogMessage($"Created connector topic: {topic}", "");
            }
            catch (Exception ex)
            {
                LogMessage($"Failed to create connector topic {topic}: {ex.Message}", "");
            }
        }

        LogMessage(
            $"Topic creation completed. Created {systemTopics.Count} system topics and {allTopics.Count} connector topics.", "");
    }

    private async Task CreateTopicAsync(string topicName, int partitions = 1, short replicationFactor = 1)
    {
        var bootstrapServers = Configuration.GetServiceEndpoint("Kafka");

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
        await base.DisposeAsync();
    }
}
