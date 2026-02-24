using Confluent.Kafka;
using Confluent.Kafka.Admin;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Microsoft.Extensions.Configuration;
using Xunit;
using Microsoft.Extensions.DependencyInjection;
using System.Text.Json;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;

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

    private const int DatabaseReadyMaxAttempts = 60;
    private const int DatabaseReadyDelayMs = 1000;

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
        KafkaConnectLogBuffer.SetSkipLogFlush(Configuration.SkipKafkaConnectLogFlush);
    }

    public void LogMessage(string message, string prefix = "")
    {
        TestLoggingService.LogMessage(message, prefix);
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
            await WaitForAllDatabasesReadyAsync();
            await CreateSqlServerDatabasesAsync();
            await RunTestConfigSetupAsync();
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
        catch (Exception ex) when (ex.Message.Contains("already exists") || ex.Message.Contains("network with name") ||
                                   ex.Message.Contains("duplicate"))
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
            LogMessage("No connector configuration files found, skipping topic creation");
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
                LogMessage($"Failed to parse config file {Path.GetFileName(configFile)}: {ex.Message}");
            }
        }

        foreach (var topic in systemTopics)
        {
            try
            {
                await CreateTopicAsync(topic, partitions: 50);
                LogMessage($"Created system topic: {topic} (50 partitions)");
            }
            catch (Exception ex)
            {
                LogMessage($"Failed to create system topic {topic}: {ex.Message}");
            }
        }

        foreach (var topic in allTopics)
        {
            try
            {
                await CreateTopicAsync(topic);
                LogMessage($"Created connector topic: {topic}");
            }
            catch (Exception ex)
            {
                LogMessage($"Failed to create connector topic {topic}: {ex.Message}");
            }
        }

        LogMessage(
            $"Topic creation completed. Created {systemTopics.Count} system topics and {allTopics.Count} connector topics.");
    }

    private async Task CreateSqlServerDatabasesAsync()
    {
        LogMessage("Creating SQL Server databases from connector configurations...");

        var configFiles = Directory.GetFiles(Path.Join(Directory.GetCurrentDirectory(), "Configurations"),
            "appsettings.*.json");

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
        var connectionString = Configuration.GetServiceEndpoint("SqlServer");

        // Connect to master database to create the target database
        var builder = new Microsoft.Data.SqlClient.SqlConnectionStringBuilder(connectionString);
        builder.InitialCatalog = "master";

        await using var connection = new Microsoft.Data.SqlClient.SqlConnection(builder.ConnectionString);
        await connection.OpenAsync();

        // Check if database exists
        var checkCommand =
            new Microsoft.Data.SqlClient.SqlCommand("SELECT COUNT(*) FROM sys.databases WHERE name = @dbName",
                connection);
        checkCommand.Parameters.AddWithValue("@dbName", databaseName);
        var exists = (int)await checkCommand.ExecuteScalarAsync() > 0;

        if (!exists)
        {
            var createCommand =
                new Microsoft.Data.SqlClient.SqlCommand($"CREATE DATABASE [{databaseName}]", connection);
            await createCommand.ExecuteNonQueryAsync();
        }
    }

    private async Task WaitForSqlServerReadyAsync()
    {
        var connectionString = Configuration.GetServiceEndpoint("SqlServer");
        var builder = new Microsoft.Data.SqlClient.SqlConnectionStringBuilder(connectionString)
        {
            InitialCatalog = "master",
            ConnectTimeout = 5
        };
        
        for (var attempt = 1; attempt <= DatabaseReadyMaxAttempts; attempt++)
        {
            try
            {
                await using var connection = new Microsoft.Data.SqlClient.SqlConnection(builder.ConnectionString);
                await connection.OpenAsync();
                
                var command = new Microsoft.Data.SqlClient.SqlCommand("SELECT @@VERSION", connection);
                await command.ExecuteScalarAsync();
                
                LogMessage($"SQL Server is ready (attempt {attempt})");
                return;
            }
            catch (Exception ex)
            {
                if (attempt == DatabaseReadyMaxAttempts)
                {
                    throw new TimeoutException($"SQL Server did not become ready after {DatabaseReadyMaxAttempts} attempts", ex);
                }
                
                LogMessage($"SQL Server not ready yet (attempt {attempt}/{DatabaseReadyMaxAttempts}): {ex.Message}");
                await Task.Delay(DatabaseReadyDelayMs);
            }
        }
    }
    
        private async Task WaitForOracleReadyAsync()
        {
            var connectionString = Configuration.GetServiceEndpoint("Oracle");
            
            for (var attempt = 1; attempt <= DatabaseReadyMaxAttempts; attempt++)
            {
                try
                {
                    await using var connection = new Oracle.ManagedDataAccess.Client.OracleConnection(connectionString);
                    await connection.OpenAsync();
                    
                    var command = new Oracle.ManagedDataAccess.Client.OracleCommand("SELECT * FROM DUAL", connection);
                    await command.ExecuteScalarAsync();
                    
                    LogMessage($"Oracle is ready (attempt {attempt})");
                    return;
                }
                catch (Exception ex)
                {
                    if (attempt == DatabaseReadyMaxAttempts)
                    {
                        throw new TimeoutException($"Oracle did not become ready after {DatabaseReadyMaxAttempts} attempts", ex);
                    }
                    
                    LogMessage($"Oracle not ready yet (attempt {attempt}/{DatabaseReadyMaxAttempts}): {ex.Message}");
                    await Task.Delay(DatabaseReadyDelayMs);
                }
            }
        }
    
        private async Task WaitForPostgresReadyAsync()
        {
            var connectionString = Configuration.GetServiceEndpoint("Postgres");
            
            for (var attempt = 1; attempt <= DatabaseReadyMaxAttempts; attempt++)
            {
                try
                {
                    await using var connection = new Npgsql.NpgsqlConnection(connectionString);
                    await connection.OpenAsync();
                    
                    var command = new Npgsql.NpgsqlCommand("SELECT version()", connection);
                    await command.ExecuteScalarAsync();
                    
                    LogMessage($"Postgres is ready (attempt {attempt})");
                    return;
                }
                catch (Exception ex)
                {
                    if (attempt == DatabaseReadyMaxAttempts)
                    {
                        throw new TimeoutException($"Postgres did not become ready after {DatabaseReadyMaxAttempts} attempts", ex);
                    }
                    
                    LogMessage($"Postgres not ready yet (attempt {attempt}/{DatabaseReadyMaxAttempts}): {ex.Message}");
                    await Task.Delay(DatabaseReadyDelayMs);
                }
            }
        }
    
        private async Task WaitForMySqlReadyAsync()
        {
            var connectionString = Configuration.GetServiceEndpoint("MySql");
            
            for (var attempt = 1; attempt <= DatabaseReadyMaxAttempts; attempt++)
            {
                try
                {
                    await using var connection = new MySql.Data.MySqlClient.MySqlConnection(connectionString);
                    await connection.OpenAsync();
                    
                    var command = new MySql.Data.MySqlClient.MySqlCommand("SELECT VERSION()", connection);
                    await command.ExecuteScalarAsync();
                    
                    LogMessage($"MySQL is ready (attempt {attempt})");
                    return;
                }
                catch (Exception ex)
                {
                    if (attempt == DatabaseReadyMaxAttempts)
                    {
                        throw new TimeoutException($"MySQL did not become ready after {DatabaseReadyMaxAttempts} attempts", ex);
                    }
                    
                    LogMessage($"MySQL not ready yet (attempt {attempt}/{DatabaseReadyMaxAttempts}): {ex.Message}");
                    await Task.Delay(DatabaseReadyDelayMs);
                }
            }
        }
    
        private async Task WaitForMariaDbReadyAsync()
        {
            var connectionString = Configuration.GetServiceEndpoint("MariaDb");
            
            for (var attempt = 1; attempt <= DatabaseReadyMaxAttempts; attempt++)
            {
                try
                {
                    await using var connection = new MySqlConnector.MySqlConnection(connectionString);
                    await connection.OpenAsync();
                    
                    var command = new MySqlConnector.MySqlCommand("SELECT VERSION()", connection);
                    await command.ExecuteScalarAsync();
                    
                    LogMessage($"MariaDB is ready (attempt {attempt})");
                    return;
                }
                catch (Exception ex)
                {
                    if (attempt == DatabaseReadyMaxAttempts)
                    {
                        throw new TimeoutException($"MariaDB did not become ready after {DatabaseReadyMaxAttempts} attempts", ex);
                    }
                    
                    LogMessage($"MariaDB not ready yet (attempt {attempt}/{DatabaseReadyMaxAttempts}): {ex.Message}");
                    await Task.Delay(DatabaseReadyDelayMs);
                }
            }
        }
    
        private async Task WaitForDynamoDbReadyAsync()
        {
            var serviceUrl = Configuration.GetServiceEndpoint("DynamoDb");
            
            for (var attempt = 1; attempt <= DatabaseReadyMaxAttempts; attempt++)
            {
                Amazon.DynamoDBv2.AmazonDynamoDBClient? client = null;
                try
                {
                    var config = new Amazon.DynamoDBv2.AmazonDynamoDBConfig
                    {
                        ServiceURL = serviceUrl,
                        AuthenticationRegion = "us-east-1",
                        UseHttp = true,
                        MaxErrorRetry = 0,
                        Timeout = TimeSpan.FromSeconds(5)
                    };
                    
                    var credentials = new Amazon.Runtime.BasicAWSCredentials("dummy", "dummy");
                    client = new Amazon.DynamoDBv2.AmazonDynamoDBClient(credentials, config);
                    
                    await client.ListTablesAsync();
                    
                    LogMessage($"DynamoDB is ready (attempt {attempt})");
                    return;
                }
                catch (Exception ex)
                {
                    if (attempt == DatabaseReadyMaxAttempts)
                    {
                        throw new TimeoutException($"DynamoDB did not become ready after {DatabaseReadyMaxAttempts} attempts", ex);
                    }
                    
                    LogMessage($"DynamoDB not ready yet (attempt {attempt}/{DatabaseReadyMaxAttempts}): {ex.Message}");
                    await Task.Delay(DatabaseReadyDelayMs);
                }
                finally
                {
                    client?.Dispose();
                }
            }
        }
        
        private async Task WaitForWorkerReadyAsync()
        {
            var workerEndpoint = Configuration.GetServiceEndpoint("Worker");
            var statusUrl = $"{workerEndpoint}/workers/status";
            
            using var httpClient = new HttpClient
            {
                Timeout = TimeSpan.FromSeconds(15)
            };

            for (var attempt = 1; attempt <= DatabaseReadyMaxAttempts; attempt++)
            {
                try
                {
                    var response = await httpClient.GetAsync(statusUrl);
                    
                    if (response.IsSuccessStatusCode)
                    {
                        var content = await response.Content.ReadAsStringAsync();
                        
                        // Parse the JSON response to check connector statuses
                        try
                        {
                            var statusDoc = JsonDocument.Parse(content);
                            
                            if (statusDoc.RootElement.TryGetProperty("status", out var status))
                            {
                                // Check worker status
                                var workerRunning = false;
                                if (status.TryGetProperty("worker", out var worker) &&
                                    worker.TryGetProperty("status", out var workerStatus))
                                {
                                    workerRunning = workerStatus.GetString() == "Running";
                                }
                                
                                // Check all connectors are running
                                var allConnectorsRunning = true;
                                var connectorStatuses = new List<string>();
                                
                                if (status.TryGetProperty("connectors", out var connectors))
                                {
                                    foreach (var connector in connectors.EnumerateArray())
                                    {
                                        if (connector.TryGetProperty("name", out var name) &&
                                            connector.TryGetProperty("status", out var connectorStatus))
                                        {
                                            var connectorName = name.GetString();
                                            var connectorStatusValue = connectorStatus.GetString();
                                            connectorStatuses.Add($"{connectorName}={connectorStatusValue}");
                                            
                                            if (connectorStatusValue != "Running")
                                            {
                                                allConnectorsRunning = false;
                                            }
                                        }
                                    }
                                }
                                
                                if (workerRunning && allConnectorsRunning && connectorStatuses.Count > 0)
                                {
                                    LogMessage($"Worker and connectors are ready (attempt {attempt}): Worker=Running, Connectors=[{string.Join(", ", connectorStatuses)}]");
                                    return;
                                }
                                
                                LogMessage($"Worker or connectors not ready yet (attempt {attempt}/{DatabaseReadyMaxAttempts}): Worker={workerRunning}, Connectors=[{string.Join(", ", connectorStatuses)}]");
                            }
                            else
                            {
                                LogMessage($"Worker response missing 'status' property (attempt {attempt}/{DatabaseReadyMaxAttempts}): {content}");
                            }
                        }
                        catch (JsonException ex)
                        {
                            LogMessage($"Failed to parse worker status JSON (attempt {attempt}/{DatabaseReadyMaxAttempts}): {ex.Message}");
                        }
                    }
                    else
                    {
                        LogMessage($"Worker not ready yet (attempt {attempt}/{DatabaseReadyMaxAttempts}): HTTP {(int)response.StatusCode}");
                    }
                }
                catch (HttpRequestException ex)
                {
                    var errorType = ex.InnerException?.GetType().Name ?? ex.GetType().Name;
                    LogMessage($"Worker endpoint not available yet (attempt {attempt}/{DatabaseReadyMaxAttempts}): {errorType}");
                }
                catch (TaskCanceledException ex) when (ex.InnerException is TimeoutException)
                {
                    LogMessage($"Worker health check timeout (attempt {attempt}/{DatabaseReadyMaxAttempts})");
                }
                catch (Exception ex)
                {
                    if (attempt == DatabaseReadyMaxAttempts)
                    {
                        throw new TimeoutException($"Worker did not become ready after {DatabaseReadyMaxAttempts} attempts", ex);
                    }
                    
                    LogMessage($"Worker not ready yet (attempt {attempt}/{DatabaseReadyMaxAttempts}): {ex.GetType().Name} - {ex.Message}");
                }
                
                await Task.Delay(DatabaseReadyDelayMs);
            }
            
            throw new TimeoutException($"Worker and connectors did not reach 'Running' status after {DatabaseReadyMaxAttempts} attempts");
        }
    
        private async Task WaitForAllDatabasesReadyAsync()
        {
            LogMessage("Waiting for all databases to be ready...");
            
            var tasks = new List<Task>
            {
                WaitForSqlServerReadyAsync(),
                WaitForOracleReadyAsync(),
                WaitForPostgresReadyAsync(),
                WaitForMySqlReadyAsync(),
                WaitForMariaDbReadyAsync(),
                WaitForDynamoDbReadyAsync()
            };
            
            await Task.WhenAll(tasks);
            
            LogMessage("All databases are ready!");
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

        if (Configuration.DebugMode)
        {
            LogMessage("=== DEBUG MODE ACTIVE ===");
            LogMessage("Skipping Kafka Connect container deployment");
            LogMessage("You can now start your Kafka Connect application manually in debug mode");
            LogMessage("All supporting infrastructure (Kafka, databases, etc.) is ready");
            return;
        }
        await CreateKafkaConnectContainerAsync();
        await Task.Delay(10000);
        if (workerConfig.WaitForHealthCheck)
        {
            await WaitForWorkerReadyAsync();
        }

        _kafkaConnectDeployed = true;
    }

    private async Task CreateKafkaConnectContainerAsync()
    {
        var config = GetWorkerConfig();
        LogMessage($"Creating Kafka Connect container: {config.Name}");
        var container = await _containerService.CreateContainerAsync(config, _network!, _loggingService);
        _containers["Worker"] = container;
        LogMessage($"Kafka Connect container started: {config.Name} -> {Configuration.GetServiceEndpoint("Worker")}");
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

            if (Configuration.DebugMode)
            {
                LogMessage("Manual teardown triggered. Proceeding with cleanup...");
            }
            else
            {
                await Task.Delay(10000);
            }

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

    private async Task RunTestConfigSetupAsync()
    {
        LogMessage("Running test configuration setup scripts...");

        var configPath = Path.Join(Directory.GetCurrentDirectory(), "data", "test-config.json");
        if (!File.Exists(configPath))
        {
            LogMessage("test-config.json not found, skipping setup");
            return;
        }

        var configContent = await File.ReadAllTextAsync(configPath);
        var configs = JsonSerializer.Deserialize<TestCaseConfig[]>(configContent,
            new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

        if (configs == null) return;

        foreach (var config in configs.Where(c => c is { Skip: false, Setup: not null }))
        {
            try
            {
                LogMessage($"Running setup scripts for target: {config.Target}");

                var setupScripts = config.Setup?.Scripts?
                    .Where(s => !string.IsNullOrWhiteSpace(s))
                    .Select(s => s)
                    .ToArray();

                if (setupScripts is { Length: > 0 })
                {
                    await ExecuteScripts(config.Target, config.Setup?.Database, setupScripts);
                }
            }
            catch (Exception ex)
            {
                LogMessage($"Failed to run setup for {config.Target}: {ex.Message}");
            }
        }

        LogMessage("Test configuration setup completed.");
    }

    private async Task ExecuteScripts(string target, string database, string[] scripts)
    {
        switch (target.ToLowerInvariant())
        {
            case "postgres":
                await ExecutePostgresScripts(database, scripts);
                break;
            case "mysql":
                await ExecuteMySqlScripts(database, scripts);
                break;
            case "mariadb":
                await ExecuteMariaDbScripts(database, scripts);
                break;
            case "sqlserver":
                await ExecuteSqlServerScripts(database, scripts);
                break;
            case "oracle":
                await ExecuteOracleScripts(database, scripts);
                break;
            case "mongo":
            case "mongodb":
                await ExecuteMongoScripts(database, scripts);
                break;
            case "dynamodb":
                await ExecuteDynamoDbScripts(scripts);
                break;
            default:
                LogMessage($"Unknown target for scripts: {target}");
                break;
        }
    }

    private async Task ExecutePostgresScripts(string database, string[] scripts)
    {
        var connectionString = Configuration.GetServiceEndpoint("Postgres");
        var builder = new Npgsql.NpgsqlConnectionStringBuilder(connectionString)
        {
            Database = database 
        };

        await using var connection = new Npgsql.NpgsqlConnection(builder.ConnectionString);
        await connection.OpenAsync();

        foreach (var script in scripts)
        {
            await using var command = new Npgsql.NpgsqlCommand(script, connection);
            await command.ExecuteNonQueryAsync();
        }
    }

    private async Task ExecuteMySqlScripts(string database, string[] scripts)
    {
        var connectionString = Configuration.GetServiceEndpoint("MySql");
        var builder = new MySql.Data.MySqlClient.MySqlConnectionStringBuilder(connectionString)
        {
            Database = database
        };

        await using var connection = new MySql.Data.MySqlClient.MySqlConnection(builder.ConnectionString);
        await connection.OpenAsync();

        foreach (var script in scripts)
        {
            await using var command = new MySql.Data.MySqlClient.MySqlCommand(script, connection);
            await command.ExecuteNonQueryAsync();
        }
    }

    private async Task ExecuteMariaDbScripts(string database, string[] scripts)
    {
        var connectionString = Configuration.GetServiceEndpoint("MariaDb");
        var builder = new MySqlConnector.MySqlConnectionStringBuilder(connectionString)
        {
            Database = database
        };

        await using var connection = new MySqlConnector.MySqlConnection(builder.ConnectionString);
        await connection.OpenAsync();

        foreach (var script in scripts)
        {
            await using var command = new MySqlConnector.MySqlCommand(script, connection);
            await command.ExecuteNonQueryAsync();
        }
    }

    private async Task ExecuteSqlServerScripts(string database, string[] scripts)
    {
        var connectionString = Configuration.GetServiceEndpoint("SqlServer");
        var builder = new Microsoft.Data.SqlClient.SqlConnectionStringBuilder(connectionString)
        {
            InitialCatalog = database
        };

        await using var connection = new Microsoft.Data.SqlClient.SqlConnection(builder.ConnectionString);
        await connection.OpenAsync();

        foreach (var script in scripts)
        {
            await using var command = new Microsoft.Data.SqlClient.SqlCommand(script, connection);
            await command.ExecuteNonQueryAsync();
        }
    }

    private async Task ExecuteOracleScripts(string database, string[] scripts)
    {
        var connectionString = Configuration.GetServiceEndpoint("Oracle");

        await using var connection = new Oracle.ManagedDataAccess.Client.OracleConnection(connectionString);
        await connection.OpenAsync();

        foreach (var script in scripts)
        {
            await using var command = new Oracle.ManagedDataAccess.Client.OracleCommand(script, connection);
            await command.ExecuteNonQueryAsync();
        }
    }

    private async Task ExecuteMongoScripts(string database, string[] scripts)
    {
        var connectionString = Configuration.GetServiceEndpoint("Mongo");
        var client = new MongoDB.Driver.MongoClient(connectionString);
        var db = client.GetDatabase(database);

        foreach (var script in scripts)
        {
            try
            {
                // Parse the script as a MongoDB command JSON document
                var commandDoc = MongoDB.Bson.BsonDocument.Parse(script);
                await db.RunCommandAsync<MongoDB.Bson.BsonDocument>(commandDoc);
            }
            catch (Exception ex)
            {
                LogMessage($"Failed to execute MongoDB command: {script} - {ex.Message}");
                throw;
            }
        }
    }

    private async Task ExecuteDynamoDbScripts(string[] scripts)
    {
        var serviceUrl = Configuration.GetServiceEndpoint("DynamoDb");
        var config = new AmazonDynamoDBConfig
        {
            ServiceURL = serviceUrl,
            AuthenticationRegion = "us-east-1",
            UseHttp = true
        };
        
        var credentials = new BasicAWSCredentials("dummy", "dummy");
        using var client = new AmazonDynamoDBClient(credentials, config);

        foreach (var script in scripts)
        {
            try
            {
                var tableConfig = JsonDocument.Parse(script);
                var root = tableConfig.RootElement;
                
                if (!root.TryGetProperty("TableName", out var tableNameElement))
                {
                    LogMessage("Missing 'TableName' property in DynamoDB script");
                    continue;
                }
                
                var tableName = tableNameElement.GetString();
                if (string.IsNullOrEmpty(tableName))
                {
                    LogMessage("Invalid table name in DynamoDB script");
                    continue;
                }

                try
                {
                    await client.DescribeTableAsync(tableName);
                    LogMessage($"DynamoDB table already exists: {tableName}");
                    continue;
                }
                catch (ResourceNotFoundException)
                {
                }

                var keySchema = new List<KeySchemaElement>();
                var attributeDefinitions = new List<AttributeDefinition>();
                
                if (root.TryGetProperty("KeySchema", out var keySchemaElement))
                {
                    foreach (var key in keySchemaElement.EnumerateArray())
                    {
                        if (key.TryGetProperty("AttributeName", out var attrName) &&
                            key.TryGetProperty("KeyType", out var keyType))
                        {
                            keySchema.Add(new KeySchemaElement
                            {
                                AttributeName = attrName.GetString(),
                                KeyType = keyType.GetString()?.ToUpperInvariant() == "RANGE"
                                    ? KeyType.RANGE
                                    : KeyType.HASH
                            });
                        }
                    }
                }
                
                if (root.TryGetProperty("AttributeDefinitions", out var attrDefsElement))
                {
                    foreach (var attr in attrDefsElement.EnumerateArray())
                    {
                        if (attr.TryGetProperty("AttributeName", out var attrName) &&
                            attr.TryGetProperty("AttributeType", out var attrType))
                        {
                            var typeString = attrType.GetString()?.ToUpperInvariant();
                            var scalarType = typeString switch
                            {
                                "N" => ScalarAttributeType.N,
                                "B" => ScalarAttributeType.B,
                                _ => ScalarAttributeType.S
                            };
                            
                            attributeDefinitions.Add(new AttributeDefinition
                            {
                                AttributeName = attrName.GetString(),
                                AttributeType = scalarType
                            });
                        }
                    }
                }

                var request = new CreateTableRequest
                {
                    TableName = tableName,
                    KeySchema = keySchema,
                    AttributeDefinitions = attributeDefinitions,
                    BillingMode = BillingMode.PAY_PER_REQUEST
                };
                
                // Handle StreamSpecification if present
                if (root.TryGetProperty("StreamSpecification", out var streamSpecElement))
                {
                    var streamSpec = new StreamSpecification();
                    
                    if (streamSpecElement.TryGetProperty("StreamEnabled", out var streamEnabled))
                    {
                        streamSpec.StreamEnabled = streamEnabled.GetBoolean();
                    }
                    
                    if (streamSpecElement.TryGetProperty("StreamViewType", out var streamViewType))
                    {
                        var viewTypeString = streamViewType.GetString()?.ToUpperInvariant();
                        streamSpec.StreamViewType = viewTypeString switch
                        {
                            "KEYS_ONLY" => StreamViewType.KEYS_ONLY,
                            "NEW_IMAGE" => StreamViewType.NEW_IMAGE,
                            "OLD_IMAGE" => StreamViewType.OLD_IMAGE,
                            "NEW_AND_OLD_IMAGES" => StreamViewType.NEW_AND_OLD_IMAGES,
                            _ => StreamViewType.NEW_AND_OLD_IMAGES
                        };
                    }
                    
                    request.StreamSpecification = streamSpec;
                }

                await client.CreateTableAsync(request);
                
                const int maxAttempts = 30;
                for (var i = 0; i < maxAttempts; i++)
                {
                    var describeResponse = await client.DescribeTableAsync(tableName);
                    if (describeResponse.Table.TableStatus == TableStatus.ACTIVE)
                    {
                        break;
                    }
                    await Task.Delay(1000);
                }
            }
            catch (Exception ex)
            {
                LogMessage($"Failed to execute DynamoDB script: {script} - {ex.Message}");
                throw;
            }
        }
    }
}

[CollectionDefinition("Integration Tests")]
[TestCaseOrderer("IntegrationTests.Kafka.Connect.Infrastructure.PriorityOrderer", "Kafka.Connect.Tests")]
public class TestCollection : ICollectionFixture<TestFixture>;
