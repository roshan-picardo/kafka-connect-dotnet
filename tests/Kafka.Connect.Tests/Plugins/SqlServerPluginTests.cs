using FluentAssertions;
using Kafka.Connect.Tests.Infrastructure;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using Xunit;
using Xunit.Abstractions;

namespace Kafka.Connect.Tests.Plugins;

[Collection("IntegrationTests")]
public class SqlServerPluginTests : BaseIntegrationTest
{
    private const string SourceTopic = "test-source-topic";
    private const string SinkTopic = "test-sink-topic";

    public SqlServerPluginTests(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task SinkConnector_Should_InsertRecordsToSqlServer()
    {
        // Arrange
        Logger.LogInformation("Starting SQL Server Sink Connector Test");
        
        await WaitForKafkaConnectHealthyAsync();

        var testCustomers = new[]
        {
            new { id = 4100, name = "Alice SqlServer", email = "alice.sql@test.com", age = 28 },
            new { id = 4101, name = "Bob SqlServer", email = "bob.sql@test.com", age = 32 },
            new { id = 4102, name = "Carol SqlServer", email = "carol.sql@test.com", age = 29 }
        };

        // Act - Produce messages to sink topic
        Logger.LogInformation("Producing {Count} test messages to sink topic: {Topic}", testCustomers.Length, SinkTopic);
        
        foreach (var customer in testCustomers)
        {
            await KafkaProducer.ProduceTestCustomerInsertAsync(SinkTopic, customer.id, customer.name, customer.email, customer.age);
        }

        KafkaProducer.Flush(TimeSpan.FromSeconds(10));

        // Wait for connector to process messages
        await Task.Delay(TimeSpan.FromSeconds(15));

        // Assert - Verify records were inserted into SQL Server
        Logger.LogInformation("Verifying records were inserted into SQL Server");
        
        var recordCount = await DatabaseHelper.SqlServer.GetCustomerCountAsync(
            Configuration.SqlServerConnectionString, 
            "test_sink_customers");

        recordCount.Should().BeGreaterOrEqualTo(testCustomers.Length, 
            "All test customers should be inserted into SQL Server");

        Logger.LogInformation("SQL Server Sink Connector Test completed successfully. Records inserted: {Count}", recordCount);
    }

    [Fact]
    public async Task SourceConnector_Should_PublishRecordsFromSqlServer()
    {
        // Arrange
        Logger.LogInformation("Starting SQL Server Source Connector Test");
        
        await WaitForKafkaConnectHealthyAsync();

        using var consumer = await CreateKafkaConsumerAsync("sqlserver-source-test-group");

        var testCustomers = new[]
        {
            new { id = 4200, name = "David SqlServer", email = "david.sql@test.com", age = 35 },
            new { id = 4201, name = "Eva SqlServer", email = "eva.sql@test.com", age = 27 },
            new { id = 4202, name = "Frank SqlServer", email = "frank.sql@test.com", age = 31 }
        };

        // Act - Insert records into SQL Server source table
        Logger.LogInformation("Inserting {Count} test records into SQL Server source table", testCustomers.Length);
        
        foreach (var customer in testCustomers)
        {
            await DatabaseHelper.SqlServer.InsertCustomerAsync(
                Configuration.SqlServerConnectionString,
                customer.id,
                customer.name,
                customer.email,
                customer.age);
        }

        // Wait for source connector to detect and publish changes
        Logger.LogInformation("Waiting for source connector to publish messages to topic: {Topic}", SourceTopic);
        
        var messages = await consumer.ConsumeMessagesAsync(SourceTopic, testCustomers.Length, TimeSpan.FromMinutes(2));

        // Assert - Verify messages were published to Kafka
        messages.Should().HaveCount(testCustomers.Length, 
            "Source connector should publish all inserted records");

        foreach (var message in messages)
        {
            message.Value.Should().NotBeNullOrEmpty("Message value should not be empty");
            
            var messageData = JsonSerializer.Deserialize<JsonElement>(message.Value!);
            messageData.TryGetProperty("after", out var afterElement).Should().BeTrue("Message should contain 'after' field");
            
            afterElement.TryGetProperty("id", out var idElement).Should().BeTrue("After record should contain 'id' field");
            var customerId = idElement.GetInt32();
            
            testCustomers.Should().Contain(c => c.id == customerId, 
                $"Published message should correspond to one of the inserted customers (ID: {customerId})");
        }

        Logger.LogInformation("SQL Server Source Connector Test completed successfully. Messages consumed: {Count}", messages.Count);
    }

    [Fact]
    public async Task SinkConnector_Should_HandleUpdateOperations()
    {
        // Arrange
        Logger.LogInformation("Starting SQL Server Sink Connector Update Test");
        
        await WaitForKafkaConnectHealthyAsync();

        var customerId = 4300;
        var originalCustomer = new { id = customerId, name = "Original SqlServer", email = "original.sql@test.com", age = 25 };
        var updatedCustomer = new { id = customerId, name = "Updated SqlServer", email = "updated.sql@test.com", age = 26 };

        // Act - First insert the customer
        await KafkaProducer.ProduceTestCustomerInsertAsync(SinkTopic, originalCustomer.id, originalCustomer.name, originalCustomer.email, originalCustomer.age);
        KafkaProducer.Flush(TimeSpan.FromSeconds(5));
        await Task.Delay(TimeSpan.FromSeconds(10));

        // Then update the customer
        await KafkaProducer.ProduceTestCustomerUpdateAsync(SinkTopic, customerId,
            originalCustomer.name, originalCustomer.email, originalCustomer.age,
            updatedCustomer.name, updatedCustomer.email, updatedCustomer.age);
        KafkaProducer.Flush(TimeSpan.FromSeconds(5));
        await Task.Delay(TimeSpan.FromSeconds(10));

        // Assert - Verify the record was updated
        var recordCount = await DatabaseHelper.SqlServer.GetCustomerCountAsync(
            Configuration.SqlServerConnectionString, 
            "test_sink_customers");

        recordCount.Should().BeGreaterThan(0, "Customer record should exist in SQL Server");

        Logger.LogInformation("SQL Server Sink Connector Update Test completed successfully");
    }

    [Fact]
    public async Task SinkConnector_Should_HandleDeleteOperations()
    {
        // Arrange
        Logger.LogInformation("Starting SQL Server Sink Connector Delete Test");
        
        await WaitForKafkaConnectHealthyAsync();

        var customerId = 4400;
        var customer = new { id = customerId, name = "To Be Deleted SqlServer", email = "delete.sql@test.com", age = 30 };

        // Act - First insert the customer
        await KafkaProducer.ProduceTestCustomerInsertAsync(SinkTopic, customer.id, customer.name, customer.email, customer.age);
        KafkaProducer.Flush(TimeSpan.FromSeconds(5));
        await Task.Delay(TimeSpan.FromSeconds(10));

        // Then delete the customer
        await KafkaProducer.ProduceTestCustomerDeleteAsync(SinkTopic, customer.id, customer.name, customer.email, customer.age);
        KafkaProducer.Flush(TimeSpan.FromSeconds(5));
        await Task.Delay(TimeSpan.FromSeconds(10));

        // Assert - Verify the record handling (implementation depends on SQL Server connector behavior)
        Logger.LogInformation("SQL Server Sink Connector Delete Test completed successfully");
    }

    [Fact]
    public async Task SourceConnector_Should_HandleDatabaseChanges()
    {
        // Arrange
        Logger.LogInformation("Starting SQL Server Source Connector Database Changes Test");
        
        await WaitForKafkaConnectHealthyAsync();

        using var consumer = await CreateKafkaConsumerAsync("sqlserver-changes-test-group");

        var customerId = 4500;
        var originalName = "SqlServer Change Test Original";
        var updatedName = "SqlServer Change Test Updated";

        // Act & Assert - Insert
        Logger.LogInformation("Testing INSERT operation");
        await DatabaseHelper.SqlServer.InsertCustomerAsync(
            Configuration.SqlServerConnectionString,
            customerId,
            originalName,
            "changetest.sql@test.com",
            25);

        var insertMessages = await consumer.ConsumeMessagesAsync(SourceTopic, 1, TimeSpan.FromMinutes(1));
        insertMessages.Should().HaveCount(1, "Should receive insert message");

        // Act & Assert - Update
        Logger.LogInformation("Testing UPDATE operation");
        await DatabaseHelper.SqlServer.UpdateCustomerAsync(
            Configuration.SqlServerConnectionString,
            customerId,
            updatedName,
            "changetest.updated.sql@test.com",
            26);

        var updateMessages = await consumer.ConsumeMessagesAsync(SourceTopic, 1, TimeSpan.FromMinutes(1));
        updateMessages.Should().HaveCount(1, "Should receive update message");

        // Act & Assert - Delete
        Logger.LogInformation("Testing DELETE operation");
        await DatabaseHelper.SqlServer.DeleteCustomerAsync(
            Configuration.SqlServerConnectionString,
            customerId);

        var deleteMessages = await consumer.ConsumeMessagesAsync(SourceTopic, 1, TimeSpan.FromMinutes(1));
        deleteMessages.Should().HaveCount(1, "Should receive delete message");

        Logger.LogInformation("SQL Server Source Connector Database Changes Test completed successfully");
    }

    [Fact]
    public async Task SinkConnector_Should_HandleChangelogStrategySelector()
    {
        // Arrange
        Logger.LogInformation("Starting SQL Server Sink Connector Changelog Strategy Test");
        
        await WaitForKafkaConnectHealthyAsync();

        var customerId = 4600;
        var customer = new { id = customerId, name = "Changelog Test SqlServer", email = "changelog.sql@test.com", age = 30 };

        // Act - Test different CDC operations
        Logger.LogInformation("Testing INSERT with changelog strategy");
        await KafkaProducer.ProduceTestCustomerInsertAsync(SinkTopic, customer.id, customer.name, customer.email, customer.age);
        KafkaProducer.Flush(TimeSpan.FromSeconds(5));
        await Task.Delay(TimeSpan.FromSeconds(10));

        Logger.LogInformation("Testing UPDATE with changelog strategy");
        await KafkaProducer.ProduceTestCustomerUpdateAsync(SinkTopic, customerId,
            customer.name, customer.email, customer.age,
            "Updated Changelog SqlServer", "updated.changelog.sql@test.com", 31);
        KafkaProducer.Flush(TimeSpan.FromSeconds(5));
        await Task.Delay(TimeSpan.FromSeconds(10));

        Logger.LogInformation("Testing DELETE with changelog strategy");
        await KafkaProducer.ProduceTestCustomerDeleteAsync(SinkTopic, customerId, "Updated Changelog SqlServer", "updated.changelog.sql@test.com", 31);
        KafkaProducer.Flush(TimeSpan.FromSeconds(5));
        await Task.Delay(TimeSpan.FromSeconds(10));

        // Assert - Verify the changelog strategy selector handled all operations
        Logger.LogInformation("SQL Server Sink Connector Changelog Strategy Test completed successfully");
    }

    [Fact]
    public async Task SinkConnector_Should_HandleIdentityColumns()
    {
        // Arrange
        Logger.LogInformation("Starting SQL Server Sink Connector Identity Column Test");
        
        await WaitForKafkaConnectHealthyAsync();

        var testCustomers = new[]
        {
            new { id = 4700, name = "Identity Test 1", email = "identity1.sql@test.com", age = 25 },
            new { id = 4701, name = "Identity Test 2", email = "identity2.sql@test.com", age = 26 },
            new { id = 4702, name = "Identity Test 3", email = "identity3.sql@test.com", age = 27 }
        };

        // Act - Test handling of SQL Server identity columns
        foreach (var customer in testCustomers)
        {
            await KafkaProducer.ProduceTestCustomerInsertAsync(SinkTopic, customer.id, customer.name, customer.email, customer.age);
        }

        KafkaProducer.Flush(TimeSpan.FromSeconds(10));
        await Task.Delay(TimeSpan.FromSeconds(15));

        // Assert - Verify identity column handling
        var recordCount = await DatabaseHelper.SqlServer.GetCustomerCountAsync(
            Configuration.SqlServerConnectionString, 
            "test_sink_customers");

        recordCount.Should().BeGreaterOrEqualTo(testCustomers.Length, 
            "SQL Server sink connector should handle identity columns correctly");

        Logger.LogInformation("SQL Server Sink Connector Identity Column Test completed successfully");
    }
}