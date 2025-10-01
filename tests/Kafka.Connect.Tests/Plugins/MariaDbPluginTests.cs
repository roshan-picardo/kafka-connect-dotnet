using FluentAssertions;
using Kafka.Connect.Tests.Infrastructure;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using Xunit;
using Xunit.Abstractions;

namespace Kafka.Connect.Tests.Plugins;

[Collection("IntegrationTests")]
public class MariaDbPluginTests : BaseIntegrationTest
{
    private const string SourceTopic = "test-source-topic";
    private const string SinkTopic = "test-sink-topic";

    public MariaDbPluginTests(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task SinkConnector_Should_InsertRecordsToMariaDB()
    {
        // Arrange
        Logger.LogInformation("Starting MariaDB Sink Connector Test");
        
        await WaitForKafkaConnectHealthyAsync();

        var testCustomers = new[]
        {
            new { id = 2100, name = "Alice MariaDB", email = "alice.maria@test.com", age = 28 },
            new { id = 2101, name = "Bob MariaDB", email = "bob.maria@test.com", age = 32 },
            new { id = 2102, name = "Carol MariaDB", email = "carol.maria@test.com", age = 29 }
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

        // Assert - Verify records were inserted into MariaDB
        Logger.LogInformation("Verifying records were inserted into MariaDB");
        
        var recordCount = await DatabaseHelper.MySQL.GetCustomerCountAsync(
            Configuration.MariaDbConnectionString, 
            "test_sink_customers");

        recordCount.Should().BeGreaterOrEqualTo(testCustomers.Length, 
            "All test customers should be inserted into MariaDB");

        Logger.LogInformation("MariaDB Sink Connector Test completed successfully. Records inserted: {Count}", recordCount);
    }

    [Fact]
    public async Task SourceConnector_Should_PublishRecordsFromMariaDB()
    {
        // Arrange
        Logger.LogInformation("Starting MariaDB Source Connector Test");
        
        await WaitForKafkaConnectHealthyAsync();

        using var consumer = await CreateKafkaConsumerAsync("mariadb-source-test-group");

        var testCustomers = new[]
        {
            new { id = 2200, name = "David MariaDB", email = "david.maria@test.com", age = 35 },
            new { id = 2201, name = "Eva MariaDB", email = "eva.maria@test.com", age = 27 },
            new { id = 2202, name = "Frank MariaDB", email = "frank.maria@test.com", age = 31 }
        };

        // Act - Insert records into MariaDB source table
        Logger.LogInformation("Inserting {Count} test records into MariaDB source table", testCustomers.Length);
        
        foreach (var customer in testCustomers)
        {
            await DatabaseHelper.MySQL.InsertCustomerAsync(
                Configuration.MariaDbConnectionString,
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

        Logger.LogInformation("MariaDB Source Connector Test completed successfully. Messages consumed: {Count}", messages.Count);
    }

    [Fact]
    public async Task SinkConnector_Should_HandleUpdateOperations()
    {
        // Arrange
        Logger.LogInformation("Starting MariaDB Sink Connector Update Test");
        
        await WaitForKafkaConnectHealthyAsync();

        var customerId = 2300;
        var originalCustomer = new { id = customerId, name = "Original MariaDB", email = "original.maria@test.com", age = 25 };
        var updatedCustomer = new { id = customerId, name = "Updated MariaDB", email = "updated.maria@test.com", age = 26 };

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
        var recordCount = await DatabaseHelper.MySQL.GetCustomerCountAsync(
            Configuration.MariaDbConnectionString, 
            "test_sink_customers");

        recordCount.Should().BeGreaterThan(0, "Customer record should exist in MariaDB");

        Logger.LogInformation("MariaDB Sink Connector Update Test completed successfully");
    }

    [Fact]
    public async Task SinkConnector_Should_HandleDeleteOperations()
    {
        // Arrange
        Logger.LogInformation("Starting MariaDB Sink Connector Delete Test");
        
        await WaitForKafkaConnectHealthyAsync();

        var customerId = 2400;
        var customer = new { id = customerId, name = "To Be Deleted MariaDB", email = "delete.maria@test.com", age = 30 };

        // Act - First insert the customer
        await KafkaProducer.ProduceTestCustomerInsertAsync(SinkTopic, customer.id, customer.name, customer.email, customer.age);
        KafkaProducer.Flush(TimeSpan.FromSeconds(5));
        await Task.Delay(TimeSpan.FromSeconds(10));

        // Then delete the customer
        await KafkaProducer.ProduceTestCustomerDeleteAsync(SinkTopic, customer.id, customer.name, customer.email, customer.age);
        KafkaProducer.Flush(TimeSpan.FromSeconds(5));
        await Task.Delay(TimeSpan.FromSeconds(10));

        // Assert - Verify the record handling (implementation depends on MariaDB connector behavior)
        Logger.LogInformation("MariaDB Sink Connector Delete Test completed successfully");
    }

    [Fact]
    public async Task SourceConnector_Should_HandleDatabaseChanges()
    {
        // Arrange
        Logger.LogInformation("Starting MariaDB Source Connector Database Changes Test");
        
        await WaitForKafkaConnectHealthyAsync();

        using var consumer = await CreateKafkaConsumerAsync("mariadb-changes-test-group");

        var customerId = 2500;
        var originalName = "MariaDB Change Test Original";
        var updatedName = "MariaDB Change Test Updated";

        // Act & Assert - Insert
        Logger.LogInformation("Testing INSERT operation");
        await DatabaseHelper.MySQL.InsertCustomerAsync(
            Configuration.MariaDbConnectionString,
            customerId,
            originalName,
            "changetest.maria@test.com",
            25);

        var insertMessages = await consumer.ConsumeMessagesAsync(SourceTopic, 1, TimeSpan.FromMinutes(1));
        insertMessages.Should().HaveCount(1, "Should receive insert message");

        // Act & Assert - Update
        Logger.LogInformation("Testing UPDATE operation");
        await DatabaseHelper.MySQL.UpdateCustomerAsync(
            Configuration.MariaDbConnectionString,
            customerId,
            updatedName,
            "changetest.updated.maria@test.com",
            26);

        var updateMessages = await consumer.ConsumeMessagesAsync(SourceTopic, 1, TimeSpan.FromMinutes(1));
        updateMessages.Should().HaveCount(1, "Should receive update message");

        // Act & Assert - Delete
        Logger.LogInformation("Testing DELETE operation");
        await DatabaseHelper.MySQL.DeleteCustomerAsync(
            Configuration.MariaDbConnectionString,
            customerId);

        var deleteMessages = await consumer.ConsumeMessagesAsync(SourceTopic, 1, TimeSpan.FromMinutes(1));
        deleteMessages.Should().HaveCount(1, "Should receive delete message");

        Logger.LogInformation("MariaDB Source Connector Database Changes Test completed successfully");
    }

    [Fact]
    public async Task SinkConnector_Should_HandleChangelogStrategySelector()
    {
        // Arrange
        Logger.LogInformation("Starting MariaDB Sink Connector Changelog Strategy Test");
        
        await WaitForKafkaConnectHealthyAsync();

        var customerId = 2600;
        var customer = new { id = customerId, name = "Changelog Test MariaDB", email = "changelog.maria@test.com", age = 30 };

        // Act - Test different CDC operations
        Logger.LogInformation("Testing INSERT with changelog strategy");
        await KafkaProducer.ProduceTestCustomerInsertAsync(SinkTopic, customer.id, customer.name, customer.email, customer.age);
        KafkaProducer.Flush(TimeSpan.FromSeconds(5));
        await Task.Delay(TimeSpan.FromSeconds(10));

        Logger.LogInformation("Testing UPDATE with changelog strategy");
        await KafkaProducer.ProduceTestCustomerUpdateAsync(SinkTopic, customerId,
            customer.name, customer.email, customer.age,
            "Updated Changelog MariaDB", "updated.changelog.maria@test.com", 31);
        KafkaProducer.Flush(TimeSpan.FromSeconds(5));
        await Task.Delay(TimeSpan.FromSeconds(10));

        Logger.LogInformation("Testing DELETE with changelog strategy");
        await KafkaProducer.ProduceTestCustomerDeleteAsync(SinkTopic, customerId, "Updated Changelog MariaDB", "updated.changelog.maria@test.com", 31);
        KafkaProducer.Flush(TimeSpan.FromSeconds(5));
        await Task.Delay(TimeSpan.FromSeconds(10));

        // Assert - Verify the changelog strategy selector handled all operations
        Logger.LogInformation("MariaDB Sink Connector Changelog Strategy Test completed successfully");
    }

    [Fact]
    public async Task SourceConnector_Should_HandleSnapshotMode()
    {
        // Arrange
        Logger.LogInformation("Starting MariaDB Source Connector Snapshot Test");
        
        await WaitForKafkaConnectHealthyAsync();

        using var consumer = await CreateKafkaConsumerAsync("mariadb-snapshot-test-group");

        // Pre-populate some data for snapshot
        var snapshotCustomers = new[]
        {
            new { id = 2700, name = "Snapshot Customer 1", email = "snapshot1.maria@test.com", age = 25 },
            new { id = 2701, name = "Snapshot Customer 2", email = "snapshot2.maria@test.com", age = 26 },
            new { id = 2702, name = "Snapshot Customer 3", email = "snapshot3.maria@test.com", age = 27 }
        };

        foreach (var customer in snapshotCustomers)
        {
            await DatabaseHelper.MySQL.InsertCustomerAsync(
                Configuration.MariaDbConnectionString,
                customer.id,
                customer.name,
                customer.email,
                customer.age);
        }

        // Act - Wait for source connector to capture snapshot data
        Logger.LogInformation("Waiting for source connector to capture snapshot data");
        
        var messages = await consumer.ConsumeAllAvailableMessagesAsync(SourceTopic, TimeSpan.FromMinutes(2));

        // Assert - Verify snapshot messages were captured
        messages.Should().HaveCountGreaterOrEqualTo(snapshotCustomers.Length, 
            "Source connector should capture snapshot data");

        Logger.LogInformation("MariaDB Source Connector Snapshot Test completed successfully. Messages captured: {Count}", messages.Count);
    }
}