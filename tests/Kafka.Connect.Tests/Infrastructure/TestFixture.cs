using MongoDB.Driver;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Microsoft.Extensions.Configuration;
using Xunit;
using Microsoft.Extensions.DependencyInjection;

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
    private IContainer? _kafkaConnectContainer;
    
    private IAdminClient? _adminClient;
    
    private IMongoClient? _mongoClient;
    private bool _kafkaConnectDeployed;

    static TestFixture()
    {
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

        // Register the container service
        var services = new ServiceCollection();
        services.AddSingleton<IContainerService, ContainerService>();
        var serviceProvider = services.BuildServiceProvider();
        _containerService = serviceProvider.GetRequiredService<IContainerService>();

        // Create TestLoggingService with simple logging
        _loggingService = new TestLoggingService();

        _loggingService.SetupTestcontainersLogging(_config.DetailedLog, _config.RawJsonLog);
    }

    public bool IsKafkaConnectDeployed => _kafkaConnectDeployed || !_config.TestContainers.Worker.Enabled;

    private void LogMessage(string message)
    {
        TestLoggingService.LogMessage(message);
    }

    public async Task InitializeAsync()
    {
        try
        {
            if (_config.SkipInfrastructure)
            {
                LogMessage("Skipping infrastructure setup (SkipInfrastructure = true)");
                LogMessage("========== KAFKA CONNECT ==========");
                KafkaConnectLogStream.SetInfrastructureReady();
                return;
            }

            LogMessage("Starting integration test infrastructure...");

            await CreateNetworkAsync();
            await CreateZookeeperContainerAsync();
            await CreateKafkaContainerAsync();
            await CreateSchemaRegistryContainerAsync();
            await CreateMongoContainerAsync();
                
            if (_config.TestContainers.Worker.Enabled)
            {
                await DeployKafkaConnectAsync();
            }

            LogMessage("Integration test infrastructure ready!");
            LogMessage("");
            LogMessage("========== KAFKA CONNECT ==========");
            KafkaConnectLogStream.SetInfrastructureReady();
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

    private async Task CreateZookeeperContainerAsync()
    {
        LogMessage($"Creating Zookeeper container: {_config.TestContainers.Zookeeper.Name}");
        _zookeeperContainer = await _containerService.CreateContainerAsync(_config.TestContainers.Zookeeper, _network!, _loggingService);
        LogMessage($"Zookeeper container started: {_config.TestContainers.Zookeeper.Name}");
    }

    private async Task CreateKafkaContainerAsync()
    {
        LogMessage($"Creating Kafka container: {_config.TestContainers.Broker.Name}");
        _kafkaContainer = await _containerService.CreateContainerAsync(_config.TestContainers.Broker, _network!, _loggingService);
        LogMessage($"Kafka container started: {_config.TestContainers.Broker.Name} -> {_config.TestContainers.Broker.Hostname}:{_kafkaContainer.GetMappedPublicPort(9092)}");
    }

    private async Task CreateSchemaRegistryContainerAsync()
    {
        LogMessage($"Creating Schema Registry container: {_config.TestContainers.SchemaRegistry.Name}");
        _schemaRegistryContainer = await _containerService.CreateContainerAsync(_config.TestContainers.SchemaRegistry, _network!, _loggingService);
        LogMessage($"Schema Registry container started: {_config.TestContainers.SchemaRegistry.Name} -> {_config.TestContainers.SchemaRegistry.Hostname}:{_schemaRegistryContainer.GetMappedPublicPort(8081)}");
    }

    private async Task CreateMongoContainerAsync()
    {
        LogMessage($"Creating MongoDB container: {_config.TestContainers.Mongo.Name}");
        _mongoContainer = await _containerService.CreateContainerAsync(_config.TestContainers.Mongo, _network!, _loggingService);
        LogMessage($"MongoDB container started: {_config.TestContainers.Mongo.Name} -> {_config.Shakedown.Mongo}");
    }

    public IMongoDatabase GetMongoDatabase(string databaseName)
    {
        _mongoClient ??= new MongoClient(_config.Shakedown.Mongo);
        return _mongoClient.GetDatabase(databaseName);
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
            ClientId = _config.TestContainers.Broker.Producer.ClientId,
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
            GroupId = _config.TestContainers.Broker.Consumer.GroupId,
            AutoOffsetReset = Enum.Parse<AutoOffsetReset>(_config.TestContainers.Broker.Consumer.AutoOffsetReset),
            EnableAutoCommit = _config.TestContainers.Broker.Consumer.EnableAutoCommit,
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
        if (_kafkaConnectDeployed || !_config.TestContainers.Worker.Enabled)
            return;

        await CreateKafkaConnectContainerAsync();

        await Task.Delay(2000);

        if (_config.TestContainers.Worker.WaitForHealthCheck)
        {
            await WaitForKafkaConnectHealthAsync();
        }

        _kafkaConnectDeployed = true;
    }

    private async Task CreateKafkaConnectContainerAsync()
    {
        LogMessage($"Creating Kafka Connect container: {_config.TestContainers.Worker.Name}");
        _kafkaConnectContainer = await _containerService.CreateKafkaConnectContainerAsync(_config.TestContainers.Worker, _network!, _loggingService);
        LogMessage($"Kafka Connect container started: {_config.TestContainers.Worker.Name} -> {_config.Shakedown.Worker}");
    }

    private async Task WaitForKafkaConnectHealthAsync()
    {
        using var httpClient = new HttpClient();
        var healthUrl = $"{_config.Shakedown.Worker}{_config.TestContainers.Worker.HealthCheckEndpoint}";
        var timeout = TimeSpan.FromSeconds(_config.TestContainers.Worker.StartupTimeoutSeconds);
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
        if (_config.SkipInfrastructure)
        {
            LogMessage("Skipping infrastructure cleanup (SkipInfrastructure = true)");
            return;
        }

        await StopContainerAsync(_kafkaConnectContainer);
        LogMessage("========== KAFKA CONNECT ==========");
        LogMessage("");
        LogMessage("Tearing down test infrastructure...");
        await DisposeContainerAsync(_kafkaConnectContainer);
        await DisposeContainerAsync(_kafkaContainer);
        await DisposeContainerAsync(_schemaRegistryContainer);
        await DisposeContainerAsync(_zookeeperContainer);
        await DisposeContainerAsync(_mongoContainer);
        if (_network != null)
        {
            LogMessage($"Cleaning up test network: {_config.TestContainers.Network.Name}");
            await _network.DisposeAsync();
        }
            
        LogMessage("All containers stopped and cleaned up!");
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
public class TestCollection : ICollectionFixture<TestFixture>
{
}