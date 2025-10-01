using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Xunit;
using Xunit.Abstractions;

namespace Kafka.Connect.Tests.Infrastructure;

public abstract class BaseIntegrationTest : IAsyncLifetime
{
    protected readonly ITestOutputHelper Output;
    protected readonly ILogger Logger;
    protected readonly IServiceProvider ServiceProvider;
    protected readonly DockerTestEnvironment DockerEnvironment;
    protected readonly KafkaTestProducer KafkaProducer;
    protected readonly TestConfiguration Configuration;

    protected BaseIntegrationTest(ITestOutputHelper output)
    {
        Output = output;
        
        // Setup configuration
        var configBuilder = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables();
        
        var config = configBuilder.Build();
        Configuration = new TestConfiguration(config);

        // Setup dependency injection
        var services = new ServiceCollection();
        ConfigureServices(services);
        ServiceProvider = services.BuildServiceProvider();

        // Get logger
        var loggerFactory = ServiceProvider.GetRequiredService<ILoggerFactory>();
        Logger = loggerFactory.CreateLogger(GetType());

        // Setup Docker environment
        DockerEnvironment = new DockerTestEnvironment(
            ServiceProvider.GetRequiredService<ILogger<DockerTestEnvironment>>());

        // Setup Kafka producer
        KafkaProducer = new KafkaTestProducer(
            Configuration.KafkaBootstrapServers,
            ServiceProvider.GetRequiredService<ILogger<KafkaTestProducer>>());
    }

    protected virtual void ConfigureServices(IServiceCollection services)
    {
        services.AddLogging(builder =>
        {
            builder.AddConsole();
            builder.AddDebug();
            builder.SetMinimumLevel(LogLevel.Information);
        });
    }

    public virtual async Task InitializeAsync()
    {
        Logger.LogInformation("Initializing integration test: {TestClass}", GetType().Name);
        
        try
        {
            // Start Docker environment
            await DockerEnvironment.StartAsync();
            
            // Wait a bit for services to fully initialize
            await Task.Delay(TimeSpan.FromSeconds(10));
            
            // Initialize databases
            await InitializeDatabasesAsync();
            
            Logger.LogInformation("Integration test initialized successfully: {TestClass}", GetType().Name);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to initialize integration test: {TestClass}", GetType().Name);
            throw;
        }
    }

    public virtual async Task DisposeAsync()
    {
        Logger.LogInformation("Disposing integration test: {TestClass}", GetType().Name);
        
        try
        {
            KafkaProducer?.Dispose();
            await DockerEnvironment.DisposeAsync();
            ServiceProvider?.GetService<IServiceScope>()?.Dispose();
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Error disposing integration test: {TestClass}", GetType().Name);
        }
    }

    protected virtual async Task InitializeDatabasesAsync()
    {
        Logger.LogInformation("Initializing test databases");

        var tasks = new List<Task>
        {
            DatabaseHelper.MongoDB.InitializeAsync(Configuration.MongoDbConnectionString, Configuration.MongoDbDatabase),
            DatabaseHelper.PostgreSQL.InitializeAsync(Configuration.PostgreSqlConnectionString),
            DatabaseHelper.MySQL.InitializeAsync(Configuration.MySqlConnectionString),
            DatabaseHelper.SqlServer.InitializeAsync(Configuration.SqlServerConnectionString)
        };

        // MariaDB uses the same helper as MySQL but with different connection string
        tasks.Add(Task.Run(async () =>
        {
            await using var connection = new MySqlConnector.MySqlConnection(Configuration.MariaDbConnectionString);
            await connection.OpenAsync();
            await DatabaseHelper.MySQL.InitializeAsync(Configuration.MariaDbConnectionString);
        }));

        await Task.WhenAll(tasks);
        Logger.LogInformation("Test databases initialized successfully");
    }

    protected async Task<KafkaTestConsumer> CreateKafkaConsumerAsync(string groupId)
    {
        var consumer = new KafkaTestConsumer(
            Configuration.KafkaBootstrapServers,
            groupId,
            ServiceProvider.GetRequiredService<ILogger<KafkaTestConsumer>>());

        return consumer;
    }

    protected async Task WaitForKafkaConnectHealthyAsync(TimeSpan? timeout = null)
    {
        timeout ??= TimeSpan.FromMinutes(2);
        var endTime = DateTime.UtcNow.Add(timeout.Value);

        Logger.LogInformation("Waiting for Kafka Connect to be healthy");

        while (DateTime.UtcNow < endTime)
        {
            try
            {
                using var httpClient = new HttpClient();
                var response = await httpClient.GetAsync($"http://localhost:6000/health");
                if (response.IsSuccessStatusCode)
                {
                    Logger.LogInformation("Kafka Connect is healthy");
                    return;
                }
            }
            catch (Exception ex)
            {
                Logger.LogDebug(ex, "Kafka Connect health check failed, retrying...");
            }

            await Task.Delay(TimeSpan.FromSeconds(5));
        }

        throw new TimeoutException("Kafka Connect did not become healthy within the specified timeout");
    }

    protected async Task<T> RetryAsync<T>(Func<Task<T>> operation, int maxAttempts = 3, TimeSpan? delay = null)
    {
        delay ??= TimeSpan.FromSeconds(1);
        Exception lastException = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                return await operation();
            }
            catch (Exception ex)
            {
                lastException = ex;
                Logger.LogWarning(ex, "Operation failed on attempt {Attempt}/{MaxAttempts}", attempt, maxAttempts);

                if (attempt < maxAttempts)
                {
                    await Task.Delay(delay.Value);
                }
            }
        }

        throw new InvalidOperationException($"Operation failed after {maxAttempts} attempts", lastException);
    }

    protected async Task RetryAsync(Func<Task> operation, int maxAttempts = 3, TimeSpan? delay = null)
    {
        await RetryAsync(async () =>
        {
            await operation();
            return true;
        }, maxAttempts, delay);
    }
}

public class TestConfiguration
{
    public TestConfiguration(IConfiguration configuration)
    {
        KafkaBootstrapServers = configuration.GetValue<string>("Kafka:BootstrapServers") ?? "localhost:9092";
        SchemaRegistryUrl = configuration.GetValue<string>("Kafka:SchemaRegistryUrl") ?? "http://localhost:28081";
        
        MongoDbConnectionString = configuration.GetValue<string>("MongoDB:ConnectionString") ?? "mongodb://admin:admin123@localhost:27017/kafka_connect_test?authSource=admin";
        MongoDbDatabase = configuration.GetValue<string>("MongoDB:Database") ?? "kafka_connect_test";
        
        PostgreSqlConnectionString = configuration.GetValue<string>("PostgreSQL:ConnectionString") ?? "Host=localhost;Port=5432;Database=kafka_connect_test;Username=postgres;Password=postgres123";
        
        MySqlConnectionString = configuration.GetValue<string>("MySQL:ConnectionString") ?? "Server=localhost;Port=3306;Database=kafka_connect_test;Uid=kafka_connect;Pwd=kafka_connect123;";
        
        MariaDbConnectionString = configuration.GetValue<string>("MariaDB:ConnectionString") ?? "Server=localhost;Port=3307;Database=kafka_connect_test;Uid=kafka_connect;Pwd=kafka_connect123;";
        
        SqlServerConnectionString = configuration.GetValue<string>("SqlServer:ConnectionString") ?? "Server=localhost,1433;Database=kafka_connect_test;User Id=sa;Password=Password123!;TrustServerCertificate=true;";
        
        KafkaConnectBaseUrl = configuration.GetValue<string>("KafkaConnect:BaseUrl") ?? "http://localhost:6000";
    }

    public string KafkaBootstrapServers { get; }
    public string SchemaRegistryUrl { get; }
    public string MongoDbConnectionString { get; }
    public string MongoDbDatabase { get; }
    public string PostgreSqlConnectionString { get; }
    public string MySqlConnectionString { get; }
    public string MariaDbConnectionString { get; }
    public string SqlServerConnectionString { get; }
    public string KafkaConnectBaseUrl { get; }
}