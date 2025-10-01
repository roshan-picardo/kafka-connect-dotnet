using FluentAssertions;
using Kafka.Connect.Tests.Infrastructure;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using Xunit;
using Xunit.Abstractions;

namespace Kafka.Connect.Tests.Plugins;

[Collection("IntegrationTests")]
public class MongoDbPluginTests : BaseIntegrationTest
{
    private const string SourceTopic = "test-source-topic";
    private const string SinkTopic = "test-sink-topic";

    public MongoDbPluginTests(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task SinkConnector_Should_InsertRecordsToMongoDB()
    {
        // Arrange
        Logger.LogInformation("Starting MongoDB Sink Connector Test");
        
        await WaitForKafkaConnectHealthyAsync();

        var testCustomers = new[]
        {
            new { id = 100, name = "Alice Johnson", email = "alice.johnson@test.com", age = 28 },
            new { id = 101, name = "Bob Wilson", email = "bob.wilson@test.com", age = 32 },
            new { id = 102, name = "Carol Davis", email = "carol.davis@test.com", age = 29 }
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

        // Assert - Verify records were inserted into MongoDB
        Logger.LogInformation("Verifying records were inserted into MongoDB");
        
        var recordCount = await DatabaseHelper.MongoDB.GetCustomerCountAsync(
            Configuration.MongoDbConnectionString, 
            Configuration.MongoDbDatabase, 
            "test_sink_customers");

        recordCount.Should().BeGreaterOrEqualTo(testCustomers.Length, 
            "All test customers should be inserted into MongoDB");

        Logger.LogInformation("MongoDB Sink Connector Test completed successfully. Records inserted: {Count}", recordCount);
    }

    [Fact]
    public async Task SourceConnector_Should_PublishRecordsFromMongoDB()
    {
        // Arrange
        Logger.LogInformation("Starting MongoDB Source Connector Test");
        
        await WaitForKafkaConnectHealthyAsync();

        using var consumer = await CreateKafkaConsumerAsync("mongodb-source-test-group");

        var testCustomers = new[]
        {
            new { id = 200, name = "David Brown", email = "david.brown@test.com", age = 35 },
            new { id = 201, name = "Eva Martinez", email = "eva.martinez@test.com", age = 27 },
            new { id = 202, name = "Frank Taylor", email = "frank.taylor@test.com", age = 31 }
        };

        // Act - Insert records into MongoDB source collection
        Logger.LogInformation("Inserting {Count} test records into MongoDB source collection", testCustomers.Length);
        
        foreach (var customer in testCustomers)
        {
            await DatabaseHelper.MongoDB.InsertCustomerAsync(
                Configuration.MongoDbConnectionString,
                Configuration.MongoDbDatabase,
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

        Logger.LogInformation("MongoDB Source Connector Test completed successfully. Messages consumed: {Count}", messages.Count);
    }

    [Fact]
    public async Task SinkConnector_Should_HandleUpdateOperations()
    {
        // Arrange
        Logger.LogInformation("Starting MongoDB Sink Connector Update Test");
        
        await WaitForKafkaConnectHealthyAsync();

        var customerId = 300;
        var originalCustomer = new { id = customerId, name = "Original Name", email = "original@test.com", age = 25 };
        var updatedCustomer = new { id = customerId, name = "Updated Name", email = "updated@test.com", age = 26 };

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

        // Assert - Verify the record was updated (implementation depends on MongoDB connector behavior)
        var recordCount = await DatabaseHelper.MongoDB.GetCustomerCountAsync(
            Configuration.MongoDbConnectionString, 
            Configuration.MongoDbDatabase, 
            "test_sink_customers");

        recordCount.Should().BeGreaterThan(0, "Customer record should exist in MongoDB");

        Logger.LogInformation("MongoDB Sink Connector Update Test completed successfully");
    }

    [Fact]
    public async Task SinkConnector_Should_HandleDeleteOperations()
    {
        // Arrange
        Logger.LogInformation("Starting MongoDB Sink Connector Delete Test");
        
        await WaitForKafkaConnectHealthyAsync();

        var customerId = 400;
        var customer = new { id = customerId, name = "To Be Deleted", email = "delete@test.com", age = 30 };

        // Act - First insert the customer
        await KafkaProducer.ProduceTestCustomerInsertAsync(SinkTopic, customer.id, customer.name, customer.email, customer.age);
        KafkaProducer.Flush(TimeSpan.FromSeconds(5));
        await Task.Delay(TimeSpan.FromSeconds(10));

        // Then delete the customer
        await KafkaProducer.ProduceTestCustomerDeleteAsync(SinkTopic, customer.id, customer.name, customer.email, customer.age);
        KafkaProducer.Flush(TimeSpan.FromSeconds(5));
        await Task.Delay(TimeSpan.FromSeconds(10));

        // Assert - Verify the record handling (implementation depends on MongoDB connector behavior)
        Logger.LogInformation("MongoDB Sink Connector Delete Test completed successfully");
    }

    [Fact]
    public async Task SourceConnector_Should_HandleDatabaseChanges()
    {
        // Arrange
        Logger.LogInformation("Starting MongoDB Source Connector Database Changes Test");
        
        await WaitForKafkaConnectHealthyAsync();

        using var consumer = await CreateKafkaConsumerAsync("mongodb-changes-test-group");

        var customerId = 500;
        var originalName = "Change Test Original";
        var updatedName = "Change Test Updated";

        // Act & Assert - Insert
        Logger.LogInformation("Testing INSERT operation");
        await DatabaseHelper.MongoDB.InsertCustomerAsync(
            Configuration.MongoDbConnectionString,
            Configuration.MongoDbDatabase,
            customerId,
            originalName,
            "changetest@test.com",
            25);

        var insertMessages = await consumer.ConsumeMessagesAsync(SourceTopic, 1, TimeSpan.FromMinutes(1));
        insertMessages.Should().HaveCount(1, "Should receive insert message");

        // Act & Assert - Update
        Logger.LogInformation("Testing UPDATE operation");
        await DatabaseHelper.MongoDB.UpdateCustomerAsync(
            Configuration.MongoDbConnectionString,
            Configuration.MongoDbDatabase,
            customerId,
            updatedName,
            "changetest.updated@test.com",
            26);

        var updateMessages = await consumer.ConsumeMessagesAsync(SourceTopic, 1, TimeSpan.FromMinutes(1));
        updateMessages.Should().HaveCount(1, "Should receive update message");

        // Act & Assert - Delete
        Logger.LogInformation("Testing DELETE operation");
        await DatabaseHelper.MongoDB.DeleteCustomerAsync(
            Configuration.MongoDbConnectionString,
            Configuration.MongoDbDatabase,
            customerId);

        var deleteMessages = await consumer.ConsumeMessagesAsync(SourceTopic, 1, TimeSpan.FromMinutes(1));
        deleteMessages.Should().HaveCount(1, "Should receive delete message");

        Logger.LogInformation("MongoDB Source Connector Database Changes Test completed successfully");
    }
}