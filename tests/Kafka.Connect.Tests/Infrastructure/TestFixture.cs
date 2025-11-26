using MongoDB.Driver;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Microsoft.Extensions.Configuration;
using Xunit;
using Microsoft.Extensions.DependencyInjection;
using System.Text.Json;
using Npgsql;

namespace IntegrationTests.Kafka.Connect.Infrastructure;

public class TestFixture : IAsyncLifetime
{
    private readonly TestConfiguration _config;
    private readonly TestLoggingService _loggingService;
    private readonly IContainerService _containerService;
    private INetwork? _network;
    
    private IContainer? _zookeeperContainer;
    private IContainer? _kafkaContainer;
    private IContainer? _schemaRegistryContainer;
    private IContainer? _mongoContainer;
    private IContainer? _postgresContainer;
    private IContainer? _kafkaConnectContainer;
    
    private IAdminClient? _adminClient;
    
    private IMongoClient? _mongoClient;
    private bool _kafkaConnectDeployed;
    private XUnitOutputSuppressor? _outputSuppressor;
    private XUnitOutputSuppressor? _errorSuppressor;

    static TestFixture()
    {
        var outputSuppressor = new XUnitOutputSuppressor(Console.Out);
        var errorSuppressor = new XUnitOutputSuppressor(Console.Error);
        Console.SetOut(outputSuppressor);
        Console.SetError(errorSuppressor);
        
        Environment.SetEnvironmentVariable("TESTCONTAINERS_RYUK_DISABLED", "false");
        Environment.SetEnvironmentVariable("TESTCONTAINERS_CHECKS_DISABLE", "false");
        Environment.SetEnvironmentVariable("TESTCONTAINERS_LOG_LEVEL", "INFO");
        Environment.SetEnvironmentVariable("KAFKA_LOG_LEVEL", "3");
    }

    public TestFixture()
    {
        var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();

        _config = new TestConfiguration();
        configuration.Bind(_config);

        var services = new ServiceCollection();
        services.AddSingleton<IContainerService, ContainerService>();
        var serviceProvider = services.BuildServiceProvider();
        _containerService = serviceProvider.GetRequiredService<IContainerService>();

        _loggingService = new TestLoggingService();

        _loggingService.SetupTestcontainersLogging(_config.DetailedLog, _config.RawJsonLog);
        
        KafkaConnectLogBuffer.SetRawJsonMode(_config.RawJsonLog);
    }

    private void LogMessage(string message)
    {
        TestLoggingService.LogMessage(message);
    }

    public async Task InitializeAsync()
    {
        try
        {
            _outputSuppressor = new XUnitOutputSuppressor(Console.Out);
            _errorSuppressor = new XUnitOutputSuppressor(Console.Error);
            Console.SetOut(_outputSuppressor);
            Console.SetError(_errorSuppressor);
            
            if (_config.SkipInfrastructure)
            {
                LogMessage("Skipping infrastructure setup (SkipInfrastructure = true)");
                return;
            }

            LogMessage("Starting integration test infrastructure...");

            await CreateNetworkAsync();
            await CreateContainersAsync();
            await CreateConnectorTopicsAsync();
            await DeployKafkaConnectAsync();

            LogMessage("Integration test infrastructure ready!");
        }
        catch (Exception ex)
        {
            TestLoggingService.LogMessage($"Failed to initialize test infrastructure: {ex.Message}");
            await DisposeAsync();
            throw;
        }
    }

    private async Task CreateNetworkAsync()
    {
        _network = new NetworkBuilder()
            .WithName(_config.TestContainers.Network.Name)
            .Build();

        LogMessage($"Creating test network: {_config.TestContainers.Network.Name}");
        await _network.CreateAsync();
    }

    private async Task CreateContainersAsync()
    {
        // Define container creation order to maintain dependencies
        var containerOrder = new[] { "Zookeeper", "Broker", "SchemaRegistry", "Mongo", "Postgres" };
        
        foreach (var containerKey in containerOrder)
        {
            if (_config.TestContainers.Containers.TryGetValue(containerKey, out var config))
            {
                var container = await _containerService.CreateContainerAsync(config, _network!, _loggingService);
                
                // Assign to appropriate field for backward compatibility
                switch (containerKey)
                {
                    case "Zookeeper":
                        _zookeeperContainer = container;
                        break;
                    case "Broker":
                        _kafkaContainer = container;
                        LogMessage($"Kafka container started: {config.Name} -> {config.Hostname}:{container.GetMappedPublicPort(9092)}");
                        break;
                    case "SchemaRegistry":
                        _schemaRegistryContainer = container;
                        LogMessage($"Schema Registry container started: {config.Name} -> {config.Hostname}:{container.GetMappedPublicPort(8081)}");
                        break;
                    case "Mongo":
                        _mongoContainer = container;
                        LogMessage($"MongoDB container started: {config.Name} -> {_config.Shakedown.Mongo}");
                        break;
                    case "Postgres":
                        _postgresContainer = container;
                        LogMessage($"PostgreSQL container started: {config.Name} -> {_config.Shakedown.Postgres}");
                        break;
                }
            }
        }
    }

    public IMongoDatabase GetMongoDatabase(string databaseName)
    {
        _mongoClient ??= new MongoClient(_config.Shakedown.Mongo);
        return _mongoClient.GetDatabase(databaseName);
    }

    public NpgsqlConnection GetPostgresConnection(string? databaseName = null)
    {
        var connectionString = _config.Shakedown.Postgres;
        if (!string.IsNullOrEmpty(databaseName))
        {
            var builder = new NpgsqlConnectionStringBuilder(connectionString)
            {
                Database = databaseName
            };
            connectionString = builder.ConnectionString;
        }
        return new NpgsqlConnection(connectionString);
    }

    private async Task CreateConnectorTopicsAsync()
    {
        LogMessage("Creating topics from connector configurations...");
        
        var configFiles = Directory.GetFiles(Path.Join(Directory.GetCurrentDirectory(), "Configurations"), "appsettings.*.json");
        
        if (configFiles.Length == 0)
        {
            LogMessage("No connector configuration files found, skipping topic creation");
            return;
        }
        var allTopics = new HashSet<string>();

        foreach (var configFile in configFiles)
        {
            try
            {
                var configContent = await File.ReadAllTextAsync(configFile);
                var configJson = JsonDocument.Parse(configContent);
                
                if (configJson.RootElement.TryGetProperty("worker", out var worker) &&
                    worker.TryGetProperty("connectors", out var connectors))
                {
                    foreach (var connector in connectors.EnumerateObject())
                    {
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
                    }
                }
            }
            catch (Exception ex)
            {
                LogMessage($"Failed to parse config file {Path.GetFileName(configFile)}: {ex.Message}");
            }
        }

        foreach (var topic in allTopics)
        {
            try
            {
                await CreateTopicAsync(topic);
                LogMessage($"Created topic: {topic}");
            }
            catch (Exception ex)
            {
                LogMessage($"Failed to create topic {topic}: {ex.Message}");
            }
        }
        
        LogMessage($"Topic creation completed. Created {allTopics.Count} topics.");
    }

    public async Task CreateTopicAsync(string topicName, int partitions = 1, short replicationFactor = 1)
    {
        var bootstrapServers = _config.Shakedown.Kafka;
            
        _adminClient ??= new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = bootstrapServers
            })
            .SetLogHandler((_, logMessage) =>
            {
                if (_config.DetailedLog)
                {
                    LogMessage(logMessage.Message);
                }
            })
            .SetErrorHandler((_, error) =>
            {
                if (_config.DetailedLog)
                {
                    LogMessage($"Kafka Admin Client Error: {error.Reason}");
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

    public async Task<DeliveryResult<string, string>> ProduceMessageAsync(string topic, string key, string value)
    {
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _config.Shakedown.Kafka,
            ClientId = _config.TestContainers.Producer.ClientId,
            SecurityProtocol = SecurityProtocol.Plaintext,
            MessageTimeoutMs = 30000,
            RequestTimeoutMs = 10000,
            DeliveryReportFields = "all",
            Acks = Acks.All,
            EnableIdempotence = true
        };

        using var producer = new ProducerBuilder<string, string>(producerConfig)
            .SetLogHandler((_, logMessage) =>
            {
                if (_config.DetailedLog)
                {
                    LogMessage(logMessage.Message);
                }
            })
            .SetErrorHandler((_, error) =>
            {
                if (_config.DetailedLog)
                {
                    LogMessage($"Kafka Producer Error: {error.Reason}");
                }
            })
            .Build();
            
        var message = new Message<string, string>
        {
            Key = key,
            Value = value
        };

        var result = await producer.ProduceAsync(topic, message);
        return result;
    }

    public async Task<ConsumeResult<string, string>> ConsumeMessageAsync(string topic, TimeSpan timeout)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _config.Shakedown.Kafka,
            GroupId = _config.TestContainers.Consumer.GroupId,
            AutoOffsetReset = Enum.Parse<AutoOffsetReset>(_config.TestContainers.Consumer.AutoOffsetReset),
            EnableAutoCommit = _config.TestContainers.Consumer.EnableAutoCommit,
            SecurityProtocol = SecurityProtocol.Plaintext,
            SessionTimeoutMs = 30000,
            MaxPollIntervalMs = 30000,
            FetchWaitMaxMs = 500
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig)
            .SetLogHandler((_, logMessage) =>
            {
                if (_config.DetailedLog)
                {
                    LogMessage(logMessage.Message);
                }
            })
            .SetErrorHandler((_, error) =>
            {
                if (_config.DetailedLog)
                {
                    LogMessage($"Kafka Consumer Error: {error.Reason}");
                }
            })
            .Build();
        consumer.Subscribe(topic);

        return await Task.Run(() =>
        {
            try
            {
                var result = consumer.Consume(timeout);
                return result;
            }
            finally
            {
                consumer.Unsubscribe();
            }
        });
    }

    public async Task DeleteTopicAsync(string topicName)
    {
        using var adminClient = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = _config.Shakedown.Kafka
        }).Build();

        try
        {
            await adminClient.DeleteTopicsAsync([topicName]);
        }
        catch (DeleteTopicsException)
        {
        }
    }

    public async Task DeployKafkaConnectAsync()
    {
        if (!_config.TestContainers.Containers.TryGetValue("Worker", out var workerConfig) ||
            _kafkaConnectDeployed || !workerConfig.Enabled)
            return;

        await CreateKafkaConnectContainerAsync();

        await Task.Delay(2000);

        if (workerConfig.WaitForHealthCheck)
        {
            await WaitForKafkaConnectHealthAsync();
        }

        _kafkaConnectDeployed = true;
    }

    private async Task CreateKafkaConnectContainerAsync()
    {
        var config = _config.TestContainers.Containers["Worker"];
        LogMessage($"Creating Kafka Connect container: {config.Name}");
        _kafkaConnectContainer = await _containerService.CreateContainerAsync(config, _network!, _loggingService);
        LogMessage($"Kafka Connect container started: {config.Name} -> {_config.Shakedown.Worker}");
    }

    private async Task WaitForKafkaConnectHealthAsync()
    {
        var workerConfig = _config.TestContainers.Containers["Worker"];
        using var httpClient = new HttpClient();
        var healthUrl = $"{_config.Shakedown.Worker}{workerConfig.HealthCheckEndpoint}";
        var timeout = TimeSpan.FromSeconds(workerConfig.StartupTimeoutSeconds);
        var cancellationToken = new CancellationTokenSource(timeout).Token;

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var response = await httpClient.GetAsync(healthUrl, cancellationToken);
                if (response.IsSuccessStatusCode)
                {
                    return;
                }
            }
            catch (HttpRequestException)
            {
            }
            catch (TaskCanceledException)
            {
                break;
            }

            await Task.Delay(1000, cancellationToken);
        }

        throw new TimeoutException($"Kafka Connect health check failed after {timeout.TotalSeconds} seconds");
    }

    public async Task DisposeAsync()
    {
        try
        {
            if (_config.SkipInfrastructure)
            {
                LogMessage("Skipping infrastructure cleanup (SkipInfrastructure = true)");
                return;
            }

            await Task.Delay(10000); 
            
            await StopContainerAsync(_kafkaConnectContainer);
            
            LogMessage("Tearing down test infrastructure...");
            await DisposeContainerAsync(_kafkaConnectContainer);
            await DisposeContainerAsync(_kafkaContainer);
            await DisposeContainerAsync(_schemaRegistryContainer);
            await DisposeContainerAsync(_zookeeperContainer);
            await DisposeContainerAsync(_mongoContainer);
            await DisposeContainerAsync(_postgresContainer);
            if (_network != null)
            {
                LogMessage($"Cleaning up test network: {_config.TestContainers.Network.Name}");
                await _network.DisposeAsync();
            }
                
            LogMessage("All containers stopped and cleaned up!");
        }
        finally
        {
            if (_outputSuppressor != null)
            {
                Console.SetOut(new StreamWriter(Console.OpenStandardOutput()) { AutoFlush = true });
                await _outputSuppressor.DisposeAsync();
            }
            if (_errorSuppressor != null)
            {
                Console.SetError(new StreamWriter(Console.OpenStandardError()) { AutoFlush = true });
                await _errorSuppressor.DisposeAsync();
            }
        }
    }

    private static async Task StopContainerAsync(IContainer? container)
    {
        if (container != null)
        {
            await container.StopAsync();
        }
    }

    private async Task DisposeContainerAsync(IContainer? container)
    {
        if (container != null)
        {
            LogMessage($"Stopping container: {container.Name}");
            await container.DisposeAsync();
        }
    }
}

[CollectionDefinition("Integration Tests")]
public class TestCollection : ICollectionFixture<TestFixture>;