using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Testcontainers.Kafka;
using Testcontainers.MongoDb;
using Testcontainers.PostgreSql;
using Testcontainers.MySql;
using Xunit;

namespace Kafka.Connect.Tests.Infrastructure;

public abstract class BaseIntegrationTest : IAsyncLifetime
{
    protected IHost Host { get; private set; } = null!;
    protected IServiceProvider Services => Host.Services;
    protected IConfiguration Configuration { get; private set; } = null!;
    protected ILogger Logger { get; private set; } = null!;

    // Test containers
    protected KafkaContainer KafkaContainer { get; private set; } = null!;
    protected MongoDbContainer MongoContainer { get; private set; } = null!;
    protected PostgreSqlContainer PostgresContainer { get; private set; } = null!;
    protected MySqlContainer MySqlContainer { get; private set; } = null!;

    // Test helpers
    protected KafkaTestProducer KafkaProducer { get; private set; } = null!;
    protected KafkaTestConsumer KafkaConsumer { get; private set; } = null!;
    protected DatabaseHelper DatabaseHelper { get; private set; } = null!;

    public virtual async Task InitializeAsync()
    {
        await StartContainersAsync();
        await SetupConfigurationAsync();
        await SetupHostAsync();
        await SetupTestHelpersAsync();
    }

    public virtual async Task DisposeAsync()
    {
        KafkaProducer?.Dispose();
        KafkaConsumer?.Dispose();
        
        if (Host != null)
        {
            await Host.StopAsync();
            Host.Dispose();
        }

        await DisposeContainersAsync();
    }

    private async Task StartContainersAsync()
    {
        // Start Kafka container
        KafkaContainer = new KafkaBuilder()
            .WithImage("confluentinc/cp-kafka:7.4.0")
            .WithEnvironment("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
            .WithEnvironment("KAFKA_CREATE_TOPICS", "test-topic:1:1")
            .Build();

        // Start MongoDB container
        MongoContainer = new MongoDbBuilder()
            .WithImage("mongo:7.0")
            .WithUsername("testuser")
            .WithPassword("testpass")
            .Build();

        // Start PostgreSQL container
        PostgresContainer = new PostgreSqlBuilder()
            .WithImage("postgres:15")
            .WithDatabase("testdb")
            .WithUsername("testuser")
            .WithPassword("testpass")
            .Build();

        // Start MySQL container
        MySqlContainer = new MySqlBuilder()
            .WithImage("mysql:8.0")
            .WithDatabase("testdb")
            .WithUsername("testuser")
            .WithPassword("testpass")
            .Build();

        // Start all containers in parallel
        await Task.WhenAll(
            KafkaContainer.StartAsync(),
            MongoContainer.StartAsync(),
            PostgresContainer.StartAsync(),
            MySqlContainer.StartAsync()
        );

        // Wait a bit for containers to be fully ready
        await Task.Delay(TimeSpan.FromSeconds(5));
    }

    private async Task SetupConfigurationAsync()
    {
        var configBuilder = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false)
            .AddJsonFile("Configurations/appsettings.Testing.json", optional: true)
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Kafka:BootstrapServers"] = KafkaContainer.GetBootstrapAddress(),
                ["MongoDB:ConnectionString"] = MongoContainer.GetConnectionString(),
                ["PostgreSQL:ConnectionString"] = PostgresContainer.GetConnectionString(),
                ["MySQL:ConnectionString"] = MySqlContainer.GetConnectionString()
            });

        Configuration = configBuilder.Build();
        await Task.CompletedTask;
    }

    private async Task SetupHostAsync()
    {
        var hostBuilder = Microsoft.Extensions.Hosting.Host.CreateDefaultBuilder()
            .ConfigureServices((context, services) =>
            {
                services.AddSingleton(Configuration);
                services.AddLogging(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Debug));
                ConfigureServices(services);
            });

        Host = hostBuilder.Build();
        Logger = Host.Services.GetRequiredService<ILogger<BaseIntegrationTest>>();
        
        await Host.StartAsync();
    }

    private async Task SetupTestHelpersAsync()
    {
        KafkaProducer = new KafkaTestProducer(KafkaContainer.GetBootstrapAddress(), Logger);
        KafkaConsumer = new KafkaTestConsumer(KafkaContainer.GetBootstrapAddress(), Logger);
        DatabaseHelper = new DatabaseHelper(Configuration, Logger);
        
        await Task.CompletedTask;
    }

    private async Task DisposeContainersAsync()
    {
        var disposeTasks = new List<Task>();

        if (KafkaContainer != null)
            disposeTasks.Add(KafkaContainer.DisposeAsync().AsTask());
        
        if (MongoContainer != null)
            disposeTasks.Add(MongoContainer.DisposeAsync().AsTask());
        
        if (PostgresContainer != null)
            disposeTasks.Add(PostgresContainer.DisposeAsync().AsTask());
        
        if (MySqlContainer != null)
            disposeTasks.Add(MySqlContainer.DisposeAsync().AsTask());

        await Task.WhenAll(disposeTasks);
    }

    protected virtual void ConfigureServices(IServiceCollection services)
    {
        // Override in derived classes to add specific services
    }

    protected async Task WaitForKafkaTopicAsync(string topicName, TimeSpan? timeout = null)
    {
        timeout ??= TimeSpan.FromSeconds(30);
        var endTime = DateTime.UtcNow.Add(timeout.Value);

        while (DateTime.UtcNow < endTime)
        {
            try
            {
                var metadata = KafkaProducer.GetMetadata(topicName);
                if (metadata != null)
                {
                    Logger.LogInformation("Topic {TopicName} is ready", topicName);
                    return;
                }
            }
            catch (Exception ex)
            {
                Logger.LogDebug("Waiting for topic {TopicName}: {Error}", topicName, ex.Message);
            }

            await Task.Delay(TimeSpan.FromSeconds(1));
        }

        throw new TimeoutException($"Topic {topicName} was not ready within {timeout}");
    }

    protected async Task<T> WaitForConditionAsync<T>(Func<Task<T>> condition, Func<T, bool> predicate, TimeSpan? timeout = null)
    {
        timeout ??= TimeSpan.FromSeconds(30);
        var endTime = DateTime.UtcNow.Add(timeout.Value);

        while (DateTime.UtcNow < endTime)
        {
            try
            {
                var result = await condition();
                if (predicate(result))
                {
                    return result;
                }
            }
            catch (Exception ex)
            {
                Logger.LogDebug("Condition check failed: {Error}", ex.Message);
            }

            await Task.Delay(TimeSpan.FromSeconds(1));
        }

        throw new TimeoutException($"Condition was not met within {timeout}");
    }
}