using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Net.Http;
using Xunit;

namespace Kafka.Connect.Tests.Infrastructure;

/// <summary>
/// Base class for integration tests that use external Docker containers
/// instead of Testcontainers. This is designed to work with docker-compose.test.yml
/// </summary>
public abstract class DockerizedIntegrationTest : IAsyncLifetime
{
    protected IHost Host { get; private set; } = null!;
    protected IServiceProvider Services => Host.Services;
    protected IConfiguration Configuration { get; private set; } = null!;
    protected ILogger Logger { get; private set; } = null!;
    protected HttpClient HttpClient { get; private set; } = null!;

    // Test helpers
    protected KafkaTestProducer KafkaProducer { get; private set; } = null!;
    protected KafkaTestConsumer KafkaConsumer { get; private set; } = null!;
    protected DatabaseHelper DatabaseHelper { get; private set; } = null!;

    // Connection details for external containers
    protected string KafkaBootstrapServers { get; private set; } = "kafka:29092";
    protected string MySqlConnectionString { get; private set; } = "Server=mysql;Port=3306;Database=testdb;Uid=testuser;Pwd=testpass;";
    protected string PostgreSqlConnectionString { get; private set; } = "Host=postgres;Port=5432;Database=testdb;Username=testuser;Password=testpass;";
    protected string MongoDbConnectionString { get; private set; } = "mongodb://testuser:testpass@mongodb:27017/testdb?authSource=admin";
    protected string KafkaConnectUrl { get; private set; } = "http://kafka-connect:8083";

    public virtual async Task InitializeAsync()
    {
        await DetectEnvironmentAsync();
        await SetupConfigurationAsync();
        await SetupHostAsync();
        await SetupTestHelpersAsync();
        await WaitForServicesAsync();
    }

    public virtual async Task DisposeAsync()
    {
        KafkaProducer?.Dispose();
        KafkaConsumer?.Dispose();
        HttpClient?.Dispose();
        
        if (Host != null)
        {
            await Host.StopAsync();
            Host.Dispose();
        }
    }

    private async Task DetectEnvironmentAsync()
    {
        // Check if we're running inside Docker (container environment)
        var isInContainer = Environment.GetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER") == "true" ||
                           File.Exists("/.dockerenv");

        if (!isInContainer)
        {
            // Running on host machine - use localhost ports
            KafkaBootstrapServers = "localhost:9092";
            MySqlConnectionString = "Server=localhost;Port=3306;Database=testdb;Uid=testuser;Pwd=testpass;";
            PostgreSqlConnectionString = "Host=localhost;Port=5432;Database=testdb;Username=testuser;Password=testpass;";
            MongoDbConnectionString = "mongodb://testuser:testpass@localhost:27017/testdb?authSource=admin";
            KafkaConnectUrl = "http://localhost:8083";
        }

        // Override with environment variables if provided
        KafkaBootstrapServers = Environment.GetEnvironmentVariable("KAFKA__BOOTSTRAPSERVERS") ?? KafkaBootstrapServers;
        MySqlConnectionString = Environment.GetEnvironmentVariable("CONNECTIONSTRINGS__MYSQL") ?? MySqlConnectionString;
        PostgreSqlConnectionString = Environment.GetEnvironmentVariable("CONNECTIONSTRINGS__POSTGRESQL") ?? PostgreSqlConnectionString;
        MongoDbConnectionString = Environment.GetEnvironmentVariable("CONNECTIONSTRINGS__MONGODB") ?? MongoDbConnectionString;
        KafkaConnectUrl = Environment.GetEnvironmentVariable("KAFKA_CONNECT_URL") ?? KafkaConnectUrl;

        await Task.CompletedTask;
    }

    private async Task SetupConfigurationAsync()
    {
        var configBuilder = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false)
            .AddJsonFile("Configurations/appsettings.Testing.json", optional: true)
            .AddEnvironmentVariables()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Kafka:BootstrapServers"] = KafkaBootstrapServers,
                ["ConnectionStrings:MongoDB"] = MongoDbConnectionString,
                ["ConnectionStrings:PostgreSQL"] = PostgreSqlConnectionString,
                ["ConnectionStrings:MySQL"] = MySqlConnectionString,
                ["KafkaConnect:BaseUrl"] = KafkaConnectUrl
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
                services.AddHttpClient();
                ConfigureServices(services);
            });

        Host = hostBuilder.Build();
        Logger = Host.Services.GetRequiredService<ILogger<DockerizedIntegrationTest>>();
        
        var httpClientFactory = Host.Services.GetRequiredService<IHttpClientFactory>();
        HttpClient = httpClientFactory.CreateClient();
        
        await Host.StartAsync();
    }

    private async Task SetupTestHelpersAsync()
    {
        KafkaProducer = new KafkaTestProducer(KafkaBootstrapServers, Logger);
        KafkaConsumer = new KafkaTestConsumer(KafkaBootstrapServers, Logger);
        DatabaseHelper = new DatabaseHelper(Configuration, Logger);
        
        await Task.CompletedTask;
    }

    private async Task WaitForServicesAsync()
    {
        Logger.LogInformation("Waiting for external services to be ready...");

        // Wait for Kafka Connect API
        await WaitForKafkaConnectAsync();
        
        // Wait for databases
        await WaitForDatabasesAsync();
        
        Logger.LogInformation("All external services are ready!");
    }

    private async Task WaitForKafkaConnectAsync()
    {
        var timeout = TimeSpan.FromMinutes(2);
        var endTime = DateTime.UtcNow.Add(timeout);

        while (DateTime.UtcNow < endTime)
        {
            try
            {
                var response = await HttpClient.GetAsync($"{KafkaConnectUrl}/health");
                if (response.IsSuccessStatusCode)
                {
                    Logger.LogInformation("Kafka Connect API is ready at {Url}", KafkaConnectUrl);
                    return;
                }
            }
            catch (Exception ex)
            {
                Logger.LogDebug("Waiting for Kafka Connect API: {Error}", ex.Message);
            }

            await Task.Delay(TimeSpan.FromSeconds(2));
        }

        throw new TimeoutException($"Kafka Connect API at {KafkaConnectUrl} was not ready within {timeout}");
    }

    private async Task WaitForDatabasesAsync()
    {
        var tasks = new List<Task>
        {
            WaitForDatabaseAsync("mysql"),
            WaitForDatabaseAsync("postgresql"),
            WaitForDatabaseAsync("mongodb")
        };

        await Task.WhenAll(tasks);
    }

    private async Task WaitForDatabaseAsync(string databaseType)
    {
        var timeout = TimeSpan.FromMinutes(1);
        var endTime = DateTime.UtcNow.Add(timeout);

        while (DateTime.UtcNow < endTime)
        {
            try
            {
                await DatabaseHelper.WaitForDatabaseReadyAsync(databaseType, TimeSpan.FromSeconds(5));
                Logger.LogInformation("{DatabaseType} database is ready", databaseType);
                return;
            }
            catch (Exception ex)
            {
                Logger.LogDebug("Waiting for {DatabaseType} database: {Error}", databaseType, ex.Message);
                await Task.Delay(TimeSpan.FromSeconds(2));
            }
        }

        Logger.LogWarning("{DatabaseType} database was not ready within {Timeout}", databaseType, timeout);
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
                // Try to produce a test message to verify topic exists
                await KafkaProducer.ProduceAsync(topicName, "test-key", "test-message");
                Logger.LogInformation("Topic {TopicName} is ready", topicName);
                return;
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

    /// <summary>
    /// Create a connector via Kafka Connect REST API
    /// </summary>
    protected async Task<bool> CreateConnectorAsync(string connectorName, object connectorConfig)
    {
        try
        {
            var json = System.Text.Json.JsonSerializer.Serialize(new
            {
                name = connectorName,
                config = connectorConfig
            });

            var content = new StringContent(json, System.Text.Encoding.UTF8, "application/json");
            var response = await HttpClient.PostAsync($"{KafkaConnectUrl}/connectors", content);

            if (response.IsSuccessStatusCode)
            {
                Logger.LogInformation("Successfully created connector: {ConnectorName}", connectorName);
                return true;
            }
            else
            {
                var errorContent = await response.Content.ReadAsStringAsync();
                Logger.LogError("Failed to create connector {ConnectorName}: {StatusCode} - {Error}", 
                    connectorName, response.StatusCode, errorContent);
                return false;
            }
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Exception creating connector {ConnectorName}", connectorName);
            return false;
        }
    }

    /// <summary>
    /// Delete a connector via Kafka Connect REST API
    /// </summary>
    protected async Task<bool> DeleteConnectorAsync(string connectorName)
    {
        try
        {
            var response = await HttpClient.DeleteAsync($"{KafkaConnectUrl}/connectors/{connectorName}");
            
            if (response.IsSuccessStatusCode || response.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                Logger.LogInformation("Successfully deleted connector: {ConnectorName}", connectorName);
                return true;
            }
            else
            {
                Logger.LogError("Failed to delete connector {ConnectorName}: {StatusCode}", 
                    connectorName, response.StatusCode);
                return false;
            }
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Exception deleting connector {ConnectorName}", connectorName);
            return false;
        }
    }

    /// <summary>
    /// Get connector status via Kafka Connect REST API
    /// </summary>
    protected async Task<string?> GetConnectorStatusAsync(string connectorName)
    {
        try
        {
            var response = await HttpClient.GetAsync($"{KafkaConnectUrl}/connectors/{connectorName}/status");
            
            if (response.IsSuccessStatusCode)
            {
                var content = await response.Content.ReadAsStringAsync();
                return content;
            }
            
            return null;
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Exception getting connector status {ConnectorName}", connectorName);
            return null;
        }
    }
}