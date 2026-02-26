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
    private WorkerFixture? _workerFixture;

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
            var testConfigs = await LoadTestConfigurationsAsync();
            InitializeFixturesAsync(testConfigs);
            await InitializeInfrastructureInParallelAsync();
            await DeployKafkaConnectWorkerAsync();

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
        _workerFixture = new WorkerFixture(Configuration, LogMessage, _containerService, _network!);
    }

    private async Task InitializeInfrastructureInParallelAsync()
    {
        LogMessage("Initializing infrastructure components in parallel...");

        // Initialize Kafka and all databases in parallel
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

    private async Task DeployKafkaConnectWorkerAsync()
    {
        if (Configuration.DebugMode)
        {
            LogMessage("=== DEBUG MODE ACTIVE ===");
            LogMessage("Skipping Kafka Connect container deployment");
            LogMessage("You can now start your Kafka Connect application manually in debug mode");
            LogMessage("All supporting infrastructure (Kafka, databases, etc.) is ready");
            return;
        }

        await _workerFixture!.InitializeAsync();
    }

    public TestConfiguration Configuration { get; }

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

            // Dispose fixtures in reverse order (worker first, then databases, then Kafka)
            if (_workerFixture != null) await _workerFixture.DisposeAsync();
            if (_dynamoDbFixture != null) await _dynamoDbFixture.DisposeAsync();
            if (_mongoDbFixture != null) await _mongoDbFixture.DisposeAsync();
            if (_oracleFixture != null) await _oracleFixture.DisposeAsync();
            if (_sqlServerFixture != null) await _sqlServerFixture.DisposeAsync();
            if (_mariaDbFixture != null) await _mariaDbFixture.DisposeAsync();
            if (_mySqlFixture != null) await _mySqlFixture.DisposeAsync();
            if (_postgresFixture != null) await _postgresFixture.DisposeAsync();
            if (_kafkaFixture != null) await _kafkaFixture.DisposeAsync();

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
