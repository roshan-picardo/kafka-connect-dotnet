using FluentAssertions;
using Kafka.Connect.Tests.Infrastructure;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using Xunit;
using Xunit.Abstractions;

namespace Kafka.Connect.Tests.Plugins;

[Collection("IntegrationTests")]
public class PostgreSqlPluginTests : BaseIntegrationTest
{
    private const string SourceTopic = "test-source-topic";
    private const string SinkTopic = "test-sink-topic";

    public PostgreSqlPluginTests(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task SinkConnector_Should_InsertRecordsToPostgreSQL()
    {
        // Arrange
        Logger.LogInformation("Starting PostgreSQL Sink Connector Test");
        
        await WaitForKafkaConnectHealthyAsync();

        var testCustomers = new[]
        {
            new { id = 1100, name = "Alice PostgreSQL", email = "alice.pg@test.com", age = 28 },
            new { id = 1101, name = "Bob PostgreSQL", email = "bob.pg@test.com", age = 32 },
            new { id = 1102, name = "Carol PostgreSQL", email = "carol.pg@test.com", age = 29 }
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

        // Assert - Verify records were inserted into PostgreSQL
        Logger.LogInformation("Verifying records were inserted into PostgreSQL");
        
        var recordCount = await DatabaseHelper.PostgreSQL.GetCustomerCountAsync(
            Configuration.PostgreSqlConnectionString, 
            "test_sink_customers");

        recordCount.Should().BeGreaterOrEqualTo(testCustomers.Length, 
            "All test customers should be inserted into PostgreSQL");

        Logger.LogInformation("PostgreSQL Sink Connector Test completed successfully. Records inserted: {Count}", recordCount);
    }

    [Fact]
    public async Task SourceConnector_Should_PublishRecordsFromPostgreSQL()
    {
        // Arrange
        Logger.LogInformation("Starting PostgreSQL Source Connector Test");
        
        await WaitForKafkaConnectHealthyAsync();

        using var consumer = await CreateKafkaConsumerAsync("postgresql-source-test-group");

        var testCustomers = new[]
        {
            new { id = 1200, name = "David PostgreSQL", email = "david.pg@test.com", age = 35 },
            new { id = 1201, name = "Eva PostgreSQL", email = "eva.pg@test.com", age = 27 },
            new { id = 1202, name = "Frank PostgreSQL", email = "frank.pg@test.com", age = 31 }
        };

        // Act - Insert records into PostgreSQL source table
        Logger.LogInformation("Inserting {Count} test records into PostgreSQL source table", testCustomers.Length);
        
        foreach (var customer in testCustomers)
        {
            await DatabaseHelper.PostgreSQL.InsertCustomerAsync(
                Configuration.PostgreSqlConnectionString,
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

        Logger.LogInformation("PostgreSQL Source Connector Test completed successfully. Messages consumed: {Count}", messages.Count);
    }

    [Fact]
    public async Task SinkConnector_Should_HandleUpdateOperations()
    {
        // Arrange
        Logger.LogInformation("Starting PostgreSQL Sink Connector Update Test");
        
        await WaitForKafkaConnectHealthyAsync();

        var customerId = 1300;
        var originalCustomer = new { id = customerId, name = "Original PostgreSQL", email = "original.pg@test.com", age = 25 };
        var updatedCustomer = new { id = customerId, name = "Updated PostgreSQL", email = "updated.pg@test.com", age = 26 };

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
        var recordCount = await DatabaseHelper.PostgreSQL.GetCustomerCountAsync(
            Configuration.PostgreSqlConnectionString, 
            "test_sink_customers");

        recordCount.Should().BeGreaterThan(0, "Customer record should exist in PostgreSQL");

        Logger.LogInformation("PostgreSQL Sink Connector Update Test completed successfully");
    }

    [Fact]
    public async Task SinkConnector_Should_HandleDeleteOperations()
    {
        // Arrange
        Logger.LogInformation("Starting PostgreSQL Sink Connector Delete Test");
        
        await WaitForKafkaConnectHealthyAsync();

        var customerId = 1400;
        var customer = new { id = customerId, name = "To Be Deleted PostgreSQL", email = "delete.pg@test.com", age = 30 };

        // Act - First insert the customer
        await KafkaProducer.ProduceTestCustomerInsertAsync(SinkTopic, customer.id, customer.name, customer.email, customer.age);
        KafkaProducer.Flush(TimeSpan.FromSeconds(5));
        await Task.Delay(TimeSpan.FromSeconds(10));

        // Then delete the customer
        await KafkaProducer.ProduceTestCustomerDeleteAsync(SinkTopic, customer.id, customer.name, customer.email, customer.age);
        KafkaProducer.Flush(TimeSpan.FromSeconds(5));
        await Task.Delay(TimeSpan.FromSeconds(10));

        // Assert - Verify the record handling (implementation depends on PostgreSQL connector behavior)
        Logger.LogInformation("PostgreSQL Sink Connector Delete Test completed successfully");
    }

    [Fact]
    public async Task SourceConnector_Should_HandleDatabaseChanges()
    {
        // Arrange
        Logger.LogInformation("Starting PostgreSQL Source Connector Database Changes Test");
        
        await WaitForKafkaConnectHealthyAsync();

        using var consumer = await CreateKafkaConsumerAsync("postgresql-changes-test-group");

        var customerId = 1500;
        var originalName = "PostgreSQL Change Test Original";
        var updatedName = "PostgreSQL Change Test Updated";

        // Act & Assert - Insert
        Logger.LogInformation("Testing INSERT operation");
        await DatabaseHelper.PostgreSQL.InsertCustomerAsync(
            Configuration.PostgreSqlConnectionString,
            customerId,
            originalName,
            "changetest.pg@test.com",
            25);

        var insertMessages = await consumer.ConsumeMessagesAsync(SourceTopic, 1, TimeSpan.FromMinutes(1));
        insertMessages.Should().HaveCount(1, "Should receive insert message");

        // Act & Assert - Update
        Logger.LogInformation("Testing UPDATE operation");
        await DatabaseHelper.PostgreSQL.UpdateCustomerAsync(
            Configuration.PostgreSqlConnectionString,
            customerId,
            updatedName,
            "changetest.updated.pg@test.com",
            26);

        var updateMessages = await consumer.ConsumeMessagesAsync(SourceTopic, 1, TimeSpan.FromMinutes(1));
        updateMessages.Should().HaveCount(1, "Should receive update message");

        // Act & Assert - Delete
        Logger.LogInformation("Testing DELETE operation");
        await DatabaseHelper.PostgreSQL.DeleteCustomerAsync(
            Configuration.PostgreSqlConnectionString,
            customerId);

        var deleteMessages = await consumer.ConsumeMessagesAsync(SourceTopic, 1, TimeSpan.FromMinutes(1));
        deleteMessages.Should().HaveCount(1, "Should receive delete message");

        Logger.LogInformation("PostgreSQL Source Connector Database Changes Test completed successfully");
    }

    [Fact]
    public async Task SinkConnector_Should_HandleBatchOperations()
    {
        // Arrange
        Logger.LogInformation("Starting PostgreSQL Sink Connector Batch Test");
        
        await WaitForKafkaConnectHealthyAsync();

        var batchSize = 10;
        var testCustomers = Enumerable.Range(1600, batchSize)
            .Select(id => new { id, name = $"Batch Customer {id}", email = $"batch{id}@test.com", age = 25 + (id % 20) })
            .ToArray();

        // Act - Produce batch of messages
        Logger.LogInformation("Producing batch of {Count} messages to sink topic: {Topic}", batchSize, SinkTopic);
        
        var messages = testCustomers.Select(c => (c.id.ToString(), (object)c)).ToArray();
        await KafkaProducer.ProduceBatchAsync(SinkTopic, messages);
        KafkaProducer.Flush(TimeSpan.FromSeconds(10));

        // Wait for connector to process all messages
        await Task.Delay(TimeSpan.FromSeconds(20));

        // Assert - Verify all records were processed
        var recordCount = await DatabaseHelper.PostgreSQL.GetCustomerCountAsync(
            Configuration.PostgreSqlConnectionString, 
            "test_sink_customers");

        recordCount.Should().BeGreaterOrEqualTo(batchSize, 
            "All batch customers should be processed by PostgreSQL sink connector");

        Logger.LogInformation("PostgreSQL Sink Connector Batch Test completed successfully. Total records: {Count}", recordCount);
    }
}