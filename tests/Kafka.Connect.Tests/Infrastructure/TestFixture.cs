using System.Collections;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Microsoft.Extensions.Configuration;
using Xunit;
using Microsoft.Extensions.DependencyInjection;
using System.Text.Json;

namespace IntegrationTests.Kafka.Connect.Infrastructure;

public class TestFixture : IAsyncLifetime
{
    private readonly TestLoggingService _loggingService;
    private readonly IContainerService _containerService;
    private INetwork? _network;
    
    private readonly Dictionary<string, IContainer> _containers = new();
    
    private IAdminClient? _adminClient;
    
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

        Configuration = new TestConfiguration();
        configuration.Bind(Configuration);

        var services = new ServiceCollection();
        services.AddSingleton<IContainerService, ContainerService>();
        var serviceProvider = services.BuildServiceProvider();
        _containerService = serviceProvider.GetRequiredService<IContainerService>();

        _loggingService = new TestLoggingService();

        _loggingService.SetupTestcontainersLogging(Configuration.DetailedLog, Configuration.RawJsonLog);
        
        KafkaConnectLogBuffer.SetRawJsonMode(Configuration.RawJsonLog);
    }

    public void LogMessage(string message)
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
            
            if (Configuration.SkipInfrastructure)
            {
                LogMessage("Skipping infrastructure setup (SkipInfrastructure = true)");
                return;
            }

            LogMessage("Starting integration test infrastructure...");

            await CreateNetworkAsync();
            await CreateContainersAsync();
            await CreateConnectorTopicsAsync();
            await CreateSqlServerDatabasesAsync();
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
            .WithName(Configuration.TestContainers.Network.Name)
            .Build();

        try
        {
            LogMessage($"Creating network: {Configuration.TestContainers.Network.Name}");
            await _network.CreateAsync();
            LogMessage($"Network created: {Configuration.TestContainers.Network.Name}");
        }
        catch (Exception ex) when (ex.Message.Contains("already exists") || ex.Message.Contains("network with name") || ex.Message.Contains("duplicate"))
        {
            LogMessage($"Network already exists, skipping creation: {Configuration.TestContainers.Network.Name}");
        }
    }

    private async Task CreateContainersAsync()
    {
        foreach (var config in Configuration.TestContainers.Containers)
        {
            if (config.Name == "worker") continue;

            if (config.Enabled)
            {
                var container = await _containerService.CreateContainerAsync(config, _network!, _loggingService);
                _containers[config.Name] = container;
            }
        }
    }
    
    public TestConfiguration Configuration { get; }

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

    private async Task CreateSqlServerDatabasesAsync()
    {
        LogMessage("Creating SQL Server databases from connector configurations...");
        
        var configFiles = Directory.GetFiles(Path.Join(Directory.GetCurrentDirectory(), "Configurations"), "appsettings.*.json");
        
        if (configFiles.Length == 0)
        {
            LogMessage("No connector configuration files found, skipping SQL Server database creation");
            return;
        }

        var sqlServerDatabases = new HashSet<string>();

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
                        if (connector.Value.TryGetProperty("plugin", out var plugin) &&
                            plugin.TryGetProperty("name", out var pluginName) &&
                            pluginName.GetString() == "sqlserver" &&
                            plugin.TryGetProperty("properties", out var properties) &&
                            properties.TryGetProperty("database", out var database))
                        {
                            var databaseName = database.GetString();
                            if (!string.IsNullOrEmpty(databaseName) && databaseName != "master")
                            {
                                sqlServerDatabases.Add(databaseName);
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

        foreach (var databaseName in sqlServerDatabases)
        {
            try
            {
                // Add a small delay to ensure SQL Server is fully ready
                await Task.Delay(5000);
                await CreateSqlServerDatabaseAsync(databaseName);
                LogMessage($"Created SQL Server database: {databaseName}");
            }
            catch (Exception ex)
            {
                LogMessage($"Failed to create SQL Server database {databaseName}: {ex.Message}");
            }
        }
        
        LogMessage($"SQL Server database creation completed. Created {sqlServerDatabases.Count} databases.");
    }

    private async Task CreateSqlServerDatabaseAsync(string databaseName)
    {
        var connectionString = Configuration.Shakedown.SqlServer;
        
        // Connect to master database to create the target database
        var builder = new Microsoft.Data.SqlClient.SqlConnectionStringBuilder(connectionString);
        builder.InitialCatalog = "master";
        
        using var connection = new Microsoft.Data.SqlClient.SqlConnection(builder.ConnectionString);
        await connection.OpenAsync();
        
        // Check if database exists
        var checkCommand = new Microsoft.Data.SqlClient.SqlCommand("SELECT COUNT(*) FROM sys.databases WHERE name = @dbName", connection);
        checkCommand.Parameters.AddWithValue("@dbName", databaseName);
        var exists = (int)await checkCommand.ExecuteScalarAsync() > 0;
        
        if (!exists)
        {
            var createCommand = new Microsoft.Data.SqlClient.SqlCommand($"CREATE DATABASE [{databaseName}]", connection);
            await createCommand.ExecuteNonQueryAsync();
        }
    }

    private async Task CreateTopicAsync(string topicName, int partitions = 1, short replicationFactor = 1)
    {
        var bootstrapServers = Configuration.Shakedown.Kafka;
            
        _adminClient ??= new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = bootstrapServers
            })
            .SetLogHandler((_, logMessage) =>
            {
                if (Configuration.DetailedLog)
                {
                    LogMessage(logMessage.Message);
                }
            })
            .SetErrorHandler((_, error) =>
            {
                if (Configuration.DetailedLog)
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

    private async Task DeployKafkaConnectAsync()
    {
        var workerConfig = GetWorkerConfig();
        if (_kafkaConnectDeployed || !workerConfig.Enabled)
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
        var config = GetWorkerConfig();
        LogMessage($"Creating Kafka Connect container: {config.Name}");
        var container = await _containerService.CreateContainerAsync(config, _network!, _loggingService);
        _containers["Worker"] = container;
        LogMessage($"Kafka Connect container started: {config.Name} -> {Configuration.Shakedown.Worker}");
    }

    private async Task WaitForKafkaConnectHealthAsync()
    {
        var workerConfig = GetWorkerConfig();
        using var httpClient = new HttpClient();
        var healthUrl = $"{Configuration.Shakedown.Worker}{workerConfig.HealthCheckEndpoint}";
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

    private ContainerConfig GetWorkerConfig()
    {
        return Configuration.TestContainers.Containers
            .FirstOrDefault(c => c.Name == "worker") ?? new ContainerConfig { Enabled = false };
    }

    public async Task DisposeAsync()
    {
        try
        {
            if (Configuration.SkipInfrastructure)
            {
                LogMessage("Skipping infrastructure cleanup (SkipInfrastructure = true)");
                return;
            }

            await Task.Delay(10000); 
            
            LogMessage("Tearing down test infrastructure...");
            
            var containerKeys = _containers.Keys.Reverse().ToList();
            foreach (var containerKey in containerKeys)
            {
                if (_containers.TryGetValue(containerKey, out var container))
                {
                    await StopContainerAsync(container);
                    await DisposeContainerAsync(container);
                }
            }
            if (_network != null)
            {
                LogMessage($"Cleaning up test network: {Configuration.TestContainers.Network.Name}");
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