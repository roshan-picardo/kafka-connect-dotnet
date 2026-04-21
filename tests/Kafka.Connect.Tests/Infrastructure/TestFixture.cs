using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Networks;
using Microsoft.Extensions.Configuration;
using Xunit;
using Microsoft.Extensions.DependencyInjection;
using System.Text.Json;
using IntegrationTests.Kafka.Connect.Infrastructure.Fixtures;

namespace IntegrationTests.Kafka.Connect.Infrastructure;

public class TestFixture : IAsyncLifetime
{
    private readonly TestLoggingService _loggingService;
    private readonly IContainerService _containerService;
    private INetwork? _network;

    private XUnitOutputSuppressor? _outputSuppressor;
    private XUnitOutputSuppressor? _errorSuppressor;

    // Infrastructure fixtures
    private KafkaFixture? _kafkaFixture;
    private PostgresFixture? _postgresFixture;
    private MySqlFixture? _mySqlFixture;
    private MariaDbFixture? _mariaDbFixture;
    private SqlServerFixture? _sqlServerFixture;
    private OracleFixture? _oracleFixture;
    private MongoDbFixture? _mongoDbFixture;
    private DynamoDbFixture? _dynamoDbFixture;
    private LeaderFixture? _leaderFixture;
    private StandaloneFixture? _standaloneFixture;
    private DistributedFixture? _distributedFixture;

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
            
            ConfigureContainersForMode();

            await BuildKafkaConnectImageAsync();
            await CreateNetworkAsync();
            var testConfigs = await LoadTestConfigurationsAsync();
            InitializeFixturesAsync(testConfigs);
            await InitializeInfrastructureInParallelAsync();
            await DeployKafkaConnectNodesAsync();

            LogMessage("Integration test infrastructure ready!");
        }
        catch (Exception ex)
        {
            TestLoggingService.LogMessage($"Failed to initialize test infrastructure: {ex.Message}");
            await DisposeAsync();
            throw;
        }
    }

    private void ConfigureContainersForMode()
    {
        LogMessage($"Configuring containers for mode: Distributed={Configuration.Distributed}, Standalone={Configuration.Standalone}");
        
        var leaderContainer = Configuration.TestContainers.Containers.FirstOrDefault(c => c.Target == "leader");
        var standaloneContainer = Configuration.TestContainers.Containers.FirstOrDefault(c => c.Target == "standalone");
        var distributedContainer = Configuration.TestContainers.Containers.FirstOrDefault(c => c.Target == "distributed");
        
        if (leaderContainer != null)
        {
            leaderContainer.Enabled = Configuration.Distributed;
        }
        
        if (standaloneContainer != null)
        {
            standaloneContainer.Enabled = Configuration.Standalone;
        }
        
        if (distributedContainer != null)
        {
            distributedContainer.Enabled = Configuration.Distributed;
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
        }
        catch (Exception ex) when (ex.Message.Contains("already exists") || ex.Message.Contains("network with name") ||
                                   ex.Message.Contains("duplicate"))
        {
            LogMessage($"Network already exists, skipping creation: {Configuration.TestContainers.Network.Name}");
        }
    }

    private async Task BuildKafkaConnectImageAsync()
    {
        if (Configuration.DebugMode)
        {
            LogMessage("Debug mode active, skipping Docker image build");
            return;
        }

        // Find any container that uses a Dockerfile (they all use the same one)
        var dockerfileContainer = Configuration.TestContainers.Containers
            .FirstOrDefault(c => !string.IsNullOrEmpty(c.DockerfilePath) && c.Enabled);

        if (dockerfileContainer != null)
        {
            await _containerService.BuildDockerImageAsync(
                dockerfileContainer.DockerfilePath!,
                "kafka-connect:latest",
                dockerfileContainer.CleanUpImage);
        }
    }

    private async Task<TestCaseConfig[]?> LoadTestConfigurationsAsync()
    {
        var configPath = Path.Join(Directory.GetCurrentDirectory(), "data", "test-config.json");
        if (!File.Exists(configPath))
        {
            LogMessage("test-config.json not found, fixtures will skip setup scripts");
            return null;
        }

        var configContent = await File.ReadAllTextAsync(configPath);
        return JsonSerializer.Deserialize<TestCaseConfig[]>(configContent,
            new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
    }

    private void InitializeFixturesAsync(TestCaseConfig[]? testConfigs)
    {
        // Initialize all fixtures with test configurations
        _kafkaFixture = new KafkaFixture(Configuration, LogMessage, _containerService, _network!);
        _postgresFixture = new PostgresFixture(Configuration, LogMessage, _containerService, _network!, testConfigs);
        _mySqlFixture = new MySqlFixture(Configuration, LogMessage, _containerService, _network!, testConfigs);
        _mariaDbFixture = new MariaDbFixture(Configuration, LogMessage, _containerService, _network!, testConfigs);
        _sqlServerFixture = new SqlServerFixture(Configuration, LogMessage, _containerService, _network!, testConfigs);
        _oracleFixture = new OracleFixture(Configuration, LogMessage, _containerService, _network!, testConfigs);
        _mongoDbFixture = new MongoDbFixture(Configuration, LogMessage, _containerService, _network!, testConfigs);
        _dynamoDbFixture = new DynamoDbFixture(Configuration, LogMessage, _containerService, _network!, testConfigs);
        _leaderFixture = new LeaderFixture(Configuration, LogMessage, _containerService, _network!);
        _standaloneFixture = new StandaloneFixture(Configuration, LogMessage, _containerService, _network!);
        _distributedFixture = new DistributedFixture(Configuration, LogMessage, _containerService, _network!);
    }

    private async Task InitializeInfrastructureInParallelAsync()
    {
        LogMessage("Initializing infrastructure components...");

        var initializationTasks = new List<Task>
        {
            _kafkaFixture!.InitializeAsync(),
            _postgresFixture!.InitializeAsync(),
            _mySqlFixture!.InitializeAsync(),
            _mariaDbFixture!.InitializeAsync(),
            _sqlServerFixture!.InitializeAsync(),
            _oracleFixture!.InitializeAsync(),
            _mongoDbFixture!.InitializeAsync(),
            _dynamoDbFixture!.InitializeAsync()
        };

        await Task.WhenAll(initializationTasks);
        LogMessage("All infrastructure components initialized!");
    }

    private async Task DeployKafkaConnectNodesAsync()
    {
        if (Configuration.DebugMode)
        {
            LogMessage("=== DEBUG MODE ACTIVE ===");
            LogMessage("Skipping Kafka Connect container deployment");
            LogMessage("You can now start your Kafka Connect application manually in debug mode");
            LogMessage("All supporting infrastructure (Kafka, databases, etc.) is ready");
            return;
        }

        var deploymentTasks = new List<Task>();

        if (Configuration.Distributed)
        {
            deploymentTasks.Add(_leaderFixture!.InitializeAsync());
            deploymentTasks.Add(_distributedFixture!.InitializeAsync());
        }
        
        if (Configuration.Standalone)
        {
            deploymentTasks.Add(_standaloneFixture!.InitializeAsync());
        }

        if (deploymentTasks.Count > 0)
        {
            await Task.WhenAll(deploymentTasks);
        }
    }

    public TestConfiguration Configuration { get; }
    
    public async Task EnsureConnectorsHealthyAsync()
    {
        if (Configuration.DebugMode || Configuration.SkipInfrastructure || !Configuration.Distributed)
        {
            return;
        }

        if (_leaderFixture != null)
        {
            var distributedEndpoint = Configuration.GetServiceEndpoint("Distributed");
            var statusUrl = $"{distributedEndpoint}/workers/status";
            
            var connectorsRestarted = false;
            
            using var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
            try
            {
                var response = await httpClient.GetAsync(statusUrl);
                if (response.IsSuccessStatusCode)
                {
                    var content = await response.Content.ReadAsStringAsync();
                    var statusDoc = System.Text.Json.JsonDocument.Parse(content);
                    
                    if (statusDoc.RootElement.TryGetProperty("status", out var status) &&
                        status.TryGetProperty("connectors", out var connectors))
                    {
                        foreach (var connector in connectors.EnumerateArray())
                        {
                            if (connector.TryGetProperty("status", out var connectorStatus) &&
                                connectorStatus.GetString() != "Running")
                            {
                                connectorsRestarted = true;
                                break;
                            }
                        }
                    }
                }
            }
            catch
            {
                // Ignore errors in pre-check
            }
            
            await _leaderFixture.WaitForWorkerReadyAsync(statusUrl, "Distributed worker", _leaderFixture.RetryFailedConnectorsAsync, silent: true);
            
            if (connectorsRestarted)
            {
                await Task.Delay(10000); 
            }
        }
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

            // Dispose Kafka Connect nodes first (in parallel)
            var kafkaConnectDisposalTasks = new List<Task>();
            if (_distributedFixture != null) kafkaConnectDisposalTasks.Add(_distributedFixture.DisposeAsync().AsTask());
            if (_standaloneFixture != null) kafkaConnectDisposalTasks.Add(_standaloneFixture.DisposeAsync().AsTask());
            if (_leaderFixture != null) kafkaConnectDisposalTasks.Add(_leaderFixture.DisposeAsync().AsTask());
            
            if (kafkaConnectDisposalTasks.Count > 0)
            {
                await Task.WhenAll(kafkaConnectDisposalTasks);
            }

            // Dispose infrastructure components in parallel
            var infrastructureDisposalTasks = new List<Task>();
            if (_dynamoDbFixture != null) infrastructureDisposalTasks.Add(_dynamoDbFixture.DisposeAsync().AsTask());
            if (_mongoDbFixture != null) infrastructureDisposalTasks.Add(_mongoDbFixture.DisposeAsync().AsTask());
            if (_oracleFixture != null) infrastructureDisposalTasks.Add(_oracleFixture.DisposeAsync().AsTask());
            if (_sqlServerFixture != null) infrastructureDisposalTasks.Add(_sqlServerFixture.DisposeAsync().AsTask());
            if (_mariaDbFixture != null) infrastructureDisposalTasks.Add(_mariaDbFixture.DisposeAsync().AsTask());
            if (_mySqlFixture != null) infrastructureDisposalTasks.Add(_mySqlFixture.DisposeAsync().AsTask());
            if (_postgresFixture != null) infrastructureDisposalTasks.Add(_postgresFixture.DisposeAsync().AsTask());
            if (_kafkaFixture != null) infrastructureDisposalTasks.Add(_kafkaFixture.DisposeAsync().AsTask());
            
            if (infrastructureDisposalTasks.Count > 0)
            {
                await Task.WhenAll(infrastructureDisposalTasks);
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

}

[CollectionDefinition("Integration Tests")]
public class TestCollection : ICollectionFixture<TestFixture>;
