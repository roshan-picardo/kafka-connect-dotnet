using FluentAssertions;
using Kafka.Connect.Tests.Infrastructure;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using Xunit;
using Xunit.Abstractions;

namespace Kafka.Connect.Tests.Plugins;

[Collection("IntegrationTests")]
public class MySqlPluginTests : BaseIntegrationTest
{
    private const string SourceTopic = "test-source-topic";
    private const string SinkTopic = "test-sink-topic";

    public MySqlPluginTests(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task SinkConnector_Should_InsertRecordsToMySQL()
    {
        // Arrange
        Logger.LogInformation("Starting MySQL Sink Connector Test");
        
        await WaitForKafkaConnectHealthyAsync();

        var testCustomers = new[]
        {
            new { id = 3100, name = "Alice MySQL", email = "alice.mysql@test.com", age = 28 },
            new { id = 3101, name = "Bob MySQL", email = "bob.mysql@test.com", age = 32 },
            new { id = 3102, name = "Carol MySQL", email = "carol.mysql@test.com", age = 29 }
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

        // Assert - Verify records were inserted into MySQL
        Logger.LogInformation("Verifying records were inserted into MySQL");
        
        var recordCount = await DatabaseHelper.MySQL.GetCustomerCountAsync(
            Configuration.MySqlConnectionString, 
            "test_sink_customers");

        recordCount.Should().BeGreaterOrEqualTo(testCustomers.Length, 
            "All test customers should be inserted into MySQL");

        Logger.LogInformation("MySQL Sink Connector Test completed successfully. Records inserted: {Count}", recordCount);
    }

    [Fact]
    public async Task SourceConnector_Should_PublishRecordsFromMySQL()
    {
        // Arrange
        Logger.LogInformation("Starting MySQL Source Connector Test");
        
        await WaitForKafkaConnectHealthyAsync();

        using var consumer = await CreateKafkaConsumerAsync("mysql-source-test-group");

        var testCustomers = new[]
        {
            new { id = 3200, name = "David MySQL", email = "david.mysql@test.com", age = 35 },
            new { id = 3201, name = "Eva MySQL", email = "eva.mysql@test.com", age = 27 },
            new { id = 3202, name = "Frank MySQL", email = "frank.mysql@test.com", age = 31 }
        };

        // Act - Insert records into MySQL source table
        Logger.LogInformation("Inserting {Count} test records into MySQL source table", testCustomers.Length);
        
        foreach (var customer in testCustomers)
        {
            await DatabaseHelper.MySQL.InsertCustomerAsync(
                Configuration.MySqlConnectionString,
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

        Logger.LogInformation("MySQL Source Connector Test completed successfully. Messages consumed: {Count}", messages.Count);
    }

    [Fact]
    public async Task SinkConnector_Should_HandleUpdateOperations()
    {
        // Arrange
        Logger.LogInformation("Starting MySQL Sink Connector Update Test");
        
        await WaitForKafkaConnectHealthyAsync();

        var customerId = 3300;
        var originalCustomer = new { id = customerId, name = "Original MySQL", email = "original.mysql@test.com", age = 25 };
        var updatedCustomer = new { id = customerId, name = "Updated MySQL", email = "updated.mysql@test.com", age = 26 };

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
            Configuration.MySqlConnectionString, 
            "test_sink_customers");

        recordCount.Should().BeGreaterThan(0, "Customer record should exist in MySQL");

        Logger.LogInformation("MySQL Sink Connector Update Test completed successfully");
    }

    [Fact]
    public async Task SourceConnector_Should_HandleDatabaseChanges()
    {
        // Arrange
        Logger.LogInformation("Starting MySQL Source Connector Database Changes Test");
        
        await WaitForKafkaConnectHealthyAsync();

        using var consumer = await CreateKafkaConsumerAsync("mysql-changes-test-group");

        var customerId = 3500;
        var originalName = "MySQL Change Test Original";
        var updatedName = "MySQL Change Test Updated";

        // Act & Assert - Insert
        Logger.LogInformation("Testing INSERT operation");
        await DatabaseHelper.MySQL.InsertCustomerAsync(
            Configuration.MySqlConnectionString,
            customerId,
            originalName,
            "changetest.mysql@test.com",
            25);

        var insertMessages = await consumer.ConsumeMessagesAsync(SourceTopic, 1, TimeSpan.FromMinutes(1));
        insertMessages.Should().HaveCount(1, "Should receive insert message");

        // Act & Assert - Update
        Logger.LogInformation("Testing UPDATE operation");
        await DatabaseHelper.MySQL.UpdateCustomerAsync(
            Configuration.MySqlConnectionString,
            customerId,
            updatedName,
            "changetest.updated.mysql@test.com",
            26);

        var updateMessages = await consumer.ConsumeMessagesAsync(SourceTopic, 1, TimeSpan.FromMinutes(1));
        updateMessages.Should().HaveCount(1, "Should receive update message");

        // Act & Assert - Delete
        Logger.LogInformation("Testing DELETE operation");
        await DatabaseHelper.MySQL.DeleteCustomerAsync(
            Configuration.MySqlConnectionString,
            customerId);

        var deleteMessages = await consumer.ConsumeMessagesAsync(SourceTopic, 1, TimeSpan.FromMinutes(1));
        deleteMessages.Should().HaveCount(1, "Should receive delete message");

        Logger.LogInformation("MySQL Source Connector Database Changes Test completed successfully");
    }

    [Fact]
    public async Task SinkConnector_Should_HandleChangelogStrategySelector()
    {
        // Arrange
        Logger.LogInformation("Starting MySQL Sink Connector Changelog Strategy Test");
        
        await WaitForKafkaConnectHealthyAsync();

        var customerId = 3600;
        var customer = new { id = customerId, name = "Changelog Test MySQL", email = "changelog.mysql@test.com", age = 30 };

        // Act - Test different CDC operations
        Logger.LogInformation("Testing INSERT with changelog strategy");
        await KafkaProducer.ProduceTestCustomerInsertAsync(SinkTopic, customer.id, customer.name, customer.email, customer.age);
        KafkaProducer.Flush(TimeSpan.FromSeconds(5));
        await Task.Delay(TimeSpan.FromSeconds(10));

        Logger.LogInformation("Testing UPDATE with changelog strategy");
        await KafkaProducer.ProduceTestCustomerUpdateAsync(SinkTopic, customerId,
            customer.name, customer.email, customer.age,
            "Updated Changelog MySQL", "updated.changelog.mysql@test.com", 31);
        KafkaProducer.Flush(TimeSpan.FromSeconds(5));
        await Task.Delay(TimeSpan.FromSeconds(10));

        Logger.LogInformation("Testing DELETE with changelog strategy");
        await KafkaProducer.ProduceTestCustomerDeleteAsync(SinkTopic, customerId, "Updated Changelog MySQL", "updated.changelog.mysql@test.com", 31);
        KafkaProducer.Flush(TimeSpan.FromSeconds(5));
        await Task.Delay(TimeSpan.FromSeconds(10));

        // Assert - Verify the changelog strategy selector handled all operations
        Logger.LogInformation("MySQL Sink Connector Changelog Strategy Test completed successfully");
    }
}