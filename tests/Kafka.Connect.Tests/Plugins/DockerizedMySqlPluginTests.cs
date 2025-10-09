using FluentAssertions;
using Kafka.Connect.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using Xunit;
using Xunit.Abstractions;

namespace Kafka.Connect.Tests.Plugins;

/// <summary>
/// Integration tests for MySQL plugin using dockerized Kafka Connect application
/// These tests require the full Docker environment to be running
/// </summary>
[Collection("DockerizedIntegrationTestCollection")]
public class DockerizedMySqlPluginTests : DockerizedIntegrationTest
{
    private readonly ITestOutputHelper _output;
    private TestDataSeeder _testDataSeeder = null!;
    private readonly string _connectorName = "test-mysql-source-connector";

    public DockerizedMySqlPluginTests(ITestOutputHelper output)
    {
        _output = output;
    }

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        _testDataSeeder = new TestDataSeeder(DatabaseHelper, Configuration, Logger);
        
        // Wait for MySQL to be ready
        await DatabaseHelper.WaitForDatabaseReadyAsync("mysql", TimeSpan.FromMinutes(2));
        
        // Seed test data
        await _testDataSeeder.SeedMySqlAsync();
        
        // Clean up any existing connector
        await DeleteConnectorAsync(_connectorName);
    }

    public override async Task DisposeAsync()
    {
        try
        {
            // Clean up connector
            await DeleteConnectorAsync(_connectorName);
            
            // Clean up test data
            await _testDataSeeder.CleanupAllDatabasesAsync();
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Failed to cleanup test resources");
        }
        
        await base.DisposeAsync();
    }

    [Fact]
    public async Task Should_Connect_To_KafkaConnect_API_Successfully()
    {
        // Arrange & Act
        var response = await HttpClient.GetAsync($"{KafkaConnectUrl}/");
        
        // Assert
        response.IsSuccessStatusCode.Should().BeTrue();
        
        var content = await response.Content.ReadAsStringAsync();
        content.Should().NotBeEmpty();
        
        Logger.LogInformation("Kafka Connect API response: {Response}", content);
    }

    [Fact]
    public async Task Should_List_Available_Connector_Plugins()
    {
        // Arrange & Act
        var response = await HttpClient.GetAsync($"{KafkaConnectUrl}/connector-plugins");
        
        // Assert
        response.IsSuccessStatusCode.Should().BeTrue();
        
        var content = await response.Content.ReadAsStringAsync();
        var plugins = JsonSerializer.Deserialize<JsonElement[]>(content);
        
        plugins.Should().NotBeEmpty();
        plugins.Should().Contain(p => 
            p.GetProperty("class").GetString()!.Contains("MySql", StringComparison.OrdinalIgnoreCase));
        
        Logger.LogInformation("Available connector plugins: {Plugins}", 
            string.Join(", ", plugins.Select(p => p.GetProperty("class").GetString())));
    }

    [Fact]
    public async Task Should_Create_MySQL_Source_Connector_Successfully()
    {
        // Arrange
        var connectorConfig = new
        {
            connector_class = "Kafka.Connect.MySql.MySqlSourceConnector",
            tasks_max = "1",
            connection_url = MySqlConnectionString,
            table_whitelist = "integration_users",
            mode = "incrementing",
            incrementing_column_name = "id",
            topic_prefix = "mysql-",
            poll_interval_ms = "5000"
        };

        // Act
        var success = await CreateConnectorAsync(_connectorName, connectorConfig);

        // Assert
        success.Should().BeTrue();
        
        // Verify connector status
        await Task.Delay(TimeSpan.FromSeconds(5)); // Allow time for connector to initialize
        
        var status = await GetConnectorStatusAsync(_connectorName);
        status.Should().NotBeNull();
        
        var statusJson = JsonSerializer.Deserialize<JsonElement>(status!);
        statusJson.GetProperty("name").GetString().Should().Be(_connectorName);
        
        Logger.LogInformation("Connector status: {Status}", status);
    }

    [Fact]
    public async Task Should_Stream_MySQL_Changes_To_Kafka()
    {
        // Arrange - Create connector first
        var connectorConfig = new
        {
            connector_class = "Kafka.Connect.MySql.MySqlSourceConnector",
            tasks_max = "1",
            connection_url = MySqlConnectionString,
            table_whitelist = "integration_users",
            mode = "incrementing",
            incrementing_column_name = "id",
            topic_prefix = "mysql-",
            poll_interval_ms = "2000"
        };

        await CreateConnectorAsync(_connectorName, connectorConfig);
        await Task.Delay(TimeSpan.FromSeconds(10)); // Allow connector to start

        var topicName = "mysql-integration_users";
        KafkaConsumer.Subscribe(topicName);

        // Act - Insert a new user to trigger change event
        var newUserId = "streaming_test_user";
        var newUser = new
        {
            id = newUserId,
            name = "Streaming Test User",
            email = "streaming.test@example.com",
            age = 28,
            department = "QA",
            salary = 60000m,
            created_at = DateTime.UtcNow,
            updated_at = DateTime.UtcNow,
            is_active = true
        };

        await DatabaseHelper.ExecuteMySqlAsync(@"
            INSERT INTO integration_users 
            (id, name, email, age, department, salary, created_at, updated_at, is_active)
            VALUES (@id, @name, @email, @age, @department, @salary, @created_at, @updated_at, @is_active)", newUser);

        // Assert - Consume the change event from Kafka
        var messages = await KafkaConsumer.ConsumeMessagesAsync(1, TimeSpan.FromSeconds(30));
        messages.Should().HaveCountGreaterOrEqualTo(1);

        var changeMessage = messages.FirstOrDefault(m => 
            m.Message.Value.Contains(newUserId, StringComparison.OrdinalIgnoreCase));
        
        changeMessage.Should().NotBeNull();
        changeMessage!.Message.Value.Should().Contain("Streaming Test User");
        
        Logger.LogInformation("Successfully streamed MySQL change to Kafka: {Message}", 
            changeMessage.Message.Value);
    }

    [Fact]
    public async Task Should_Handle_MySQL_Connector_Lifecycle()
    {
        // Arrange
        var connectorConfig = new
        {
            connector_class = "Kafka.Connect.MySql.MySqlSourceConnector",
            tasks_max = "1",
            connection_url = MySqlConnectionString,
            table_whitelist = "integration_users",
            mode = "incrementing",
            incrementing_column_name = "id",
            topic_prefix = "mysql-lifecycle-",
            poll_interval_ms = "5000"
        };

        // Act & Assert - Create connector
        var createSuccess = await CreateConnectorAsync(_connectorName, connectorConfig);
        createSuccess.Should().BeTrue();

        // Verify connector is running
        await Task.Delay(TimeSpan.FromSeconds(5));
        var status = await GetConnectorStatusAsync(_connectorName);
        status.Should().NotBeNull();

        // Pause connector
        var pauseResponse = await HttpClient.PutAsync($"{KafkaConnectUrl}/connectors/{_connectorName}/pause", null);
        pauseResponse.IsSuccessStatusCode.Should().BeTrue();

        // Resume connector
        var resumeResponse = await HttpClient.PutAsync($"{KafkaConnectUrl}/connectors/{_connectorName}/resume", null);
        resumeResponse.IsSuccessStatusCode.Should().BeTrue();

        // Delete connector
        var deleteSuccess = await DeleteConnectorAsync(_connectorName);
        deleteSuccess.Should().BeTrue();

        // Verify connector is deleted
        var finalStatus = await GetConnectorStatusAsync(_connectorName);
        finalStatus.Should().BeNull();
        
        Logger.LogInformation("Successfully completed connector lifecycle test");
    }

    [Fact]
    public async Task Should_Handle_Connector_Configuration_Updates()
    {
        // Arrange - Create initial connector
        var initialConfig = new
        {
            connector_class = "Kafka.Connect.MySql.MySqlSourceConnector",
            tasks_max = "1",
            connection_url = MySqlConnectionString,
            table_whitelist = "integration_users",
            mode = "incrementing",
            incrementing_column_name = "id",
            topic_prefix = "mysql-config-",
            poll_interval_ms = "10000"
        };

        await CreateConnectorAsync(_connectorName, initialConfig);
        await Task.Delay(TimeSpan.FromSeconds(5));

        // Act - Update connector configuration
        var updatedConfig = new
        {
            connector_class = "Kafka.Connect.MySql.MySqlSourceConnector",
            tasks_max = "1",
            connection_url = MySqlConnectionString,
            table_whitelist = "integration_users",
            mode = "incrementing",
            incrementing_column_name = "id",
            topic_prefix = "mysql-config-",
            poll_interval_ms = "5000" // Changed from 10000 to 5000
        };

        var json = JsonSerializer.Serialize(updatedConfig);
        var content = new StringContent(json, System.Text.Encoding.UTF8, "application/json");
        var updateResponse = await HttpClient.PutAsync($"{KafkaConnectUrl}/connectors/{_connectorName}/config", content);

        // Assert
        updateResponse.IsSuccessStatusCode.Should().BeTrue();

        // Verify configuration was updated
        var configResponse = await HttpClient.GetAsync($"{KafkaConnectUrl}/connectors/{_connectorName}/config");
        configResponse.IsSuccessStatusCode.Should().BeTrue();

        var configContent = await configResponse.Content.ReadAsStringAsync();
        var config = JsonSerializer.Deserialize<JsonElement>(configContent);
        
        config.GetProperty("poll.interval.ms").GetString().Should().Be("5000");
        
        Logger.LogInformation("Successfully updated connector configuration");
    }

    protected override void ConfigureServices(IServiceCollection services)
    {
        services.AddSingleton<TestDataSeeder>();
    }
}

/// <summary>
/// Collection definition for dockerized integration tests
/// </summary>
[CollectionDefinition("DockerizedIntegrationTestCollection")]
public class DockerizedIntegrationTestCollection : ICollectionFixture<DockerizedIntegrationTestFixture>
{
}

/// <summary>
/// Shared fixture for dockerized integration tests
/// </summary>
public class DockerizedIntegrationTestFixture : IAsyncLifetime
{
    public async Task InitializeAsync()
    {
        // Global setup for dockerized integration tests
        await Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        // Global cleanup for dockerized integration tests
        await Task.CompletedTask;
    }
}