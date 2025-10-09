using FluentAssertions;
using Kafka.Connect.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using System.Text.Json;
using MongoDB.Bson;
using Xunit;
using Xunit.Abstractions;

namespace Kafka.Connect.Tests.Plugins;

[Collection("IntegrationTestCollection")]
public class MongoDbPluginTests : BaseIntegrationTest
{
    private readonly ITestOutputHelper _output;
    private TestDataSeeder _testDataSeeder = null!;

    public MongoDbPluginTests(ITestOutputHelper output)
    {
        _output = output;
    }

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        _testDataSeeder = new TestDataSeeder(DatabaseHelper, Configuration, Logger);
        
        // Wait for MongoDB to be ready
        await DatabaseHelper.WaitForDatabaseReadyAsync("mongodb", TimeSpan.FromMinutes(2));
        
        // Seed test data
        await _testDataSeeder.SeedMongoDbAsync();
    }

    public override async Task DisposeAsync()
    {
        try
        {
            await _testDataSeeder.CleanupAllDatabasesAsync();
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Failed to cleanup test data");
        }
        
        await base.DisposeAsync();
    }

    [Fact]
    public async Task Should_Connect_To_MongoDB_Successfully()
    {
        // Arrange & Act
        var database = DatabaseHelper.GetMongoDatabase("integration_testdb");
        var result = await database.RunCommandAsync<object>("{ ping: 1 }");

        // Assert
        result.Should().NotBeNull();
        Logger.LogInformation("MongoDB connection test passed");
    }

    [Fact]
    public async Task Should_Read_Users_From_MongoDB_Collection()
    {
        // Arrange
        var collection = await DatabaseHelper.GetMongoCollectionAsync<MongoUser>("integration_users", "integration_testdb");

        // Act
        var users = await collection.Find(Builders<MongoUser>.Filter.Empty).ToListAsync();

        // Assert
        users.Should().NotBeEmpty();
        users.Should().HaveCountGreaterOrEqualTo(2);
        
        var johnDoe = users.FirstOrDefault(u => u.Email == "john.doe@example.com");
        johnDoe.Should().NotBeNull();
        johnDoe!.Name.Should().Be("John Doe");
        johnDoe.Department.Should().Be("Engineering");
        johnDoe.Age.Should().Be(30);
        
        Logger.LogInformation("Found {Count} users in MongoDB", users.Count);
    }

    [Fact]
    public async Task Should_Insert_New_User_Into_MongoDB()
    {
        // Arrange
        var newUser = new MongoUser
        {
            Id = "test_user_" + Guid.NewGuid().ToString("N")[..8],
            Name = "Test User",
            Email = $"test.user.{Guid.NewGuid():N}@example.com",
            Age = 28,
            Department = "QA",
            Salary = 60000,
            CreatedAt = DateTime.UtcNow,
            UpdatedAt = DateTime.UtcNow,
            IsActive = true,
            Tags = new[] { "tester", "automation" },
            Address = new Address
            {
                Street = "123 Test St",
                City = "Test City",
                State = "TS",
                ZipCode = "12345",
                Country = "USA"
            }
        };

        // Act
        await DatabaseHelper.InsertMongoDocumentAsync("integration_users", newUser, "integration_testdb");

        // Assert
        var insertedUser = await DatabaseHelper.FindMongoDocumentsAsync<MongoUser>(
            "integration_users", 
            Builders<MongoUser>.Filter.Eq(u => u.Id, newUser.Id), 
            "integration_testdb");

        insertedUser.Should().HaveCount(1);
        insertedUser[0].Name.Should().Be("Test User");
        insertedUser[0].Department.Should().Be("QA");
        
        Logger.LogInformation("Successfully inserted and verified new user: {UserId}", newUser.Id);
    }

    [Fact]
    public async Task Should_Update_User_In_MongoDB()
    {
        // Arrange
        var collection = await DatabaseHelper.GetMongoCollectionAsync<MongoUser>("integration_users", "integration_testdb");
        var existingUser = await collection.Find(u => u.Email == "john.doe@example.com").FirstOrDefaultAsync();
        existingUser.Should().NotBeNull();

        var updatedSalary = 80000m;
        var updatedDepartment = "Senior Engineering";

        // Act
        var updateDefinition = Builders<MongoUser>.Update
            .Set(u => u.Salary, updatedSalary)
            .Set(u => u.Department, updatedDepartment)
            .Set(u => u.UpdatedAt, DateTime.UtcNow);

        await collection.UpdateOneAsync(
            Builders<MongoUser>.Filter.Eq(u => u.Id, existingUser!.Id),
            updateDefinition);

        // Assert
        var updatedUser = await collection.Find(u => u.Id == existingUser.Id).FirstOrDefaultAsync();
        updatedUser.Should().NotBeNull();
        updatedUser!.Salary.Should().Be(updatedSalary);
        updatedUser.Department.Should().Be(updatedDepartment);
        updatedUser.UpdatedAt.Should().BeAfter(existingUser.UpdatedAt);
        
        Logger.LogInformation("Successfully updated user: {UserId}", existingUser.Id);
    }

    [Fact]
    public async Task Should_Delete_User_From_MongoDB()
    {
        // Arrange
        var testUser = new MongoUser
        {
            Id = "delete_test_user",
            Name = "Delete Test User",
            Email = "delete.test@example.com",
            Age = 25,
            Department = "Test",
            Salary = 50000,
            CreatedAt = DateTime.UtcNow,
            UpdatedAt = DateTime.UtcNow,
            IsActive = true,
            Tags = Array.Empty<string>(),
            Address = new Address()
        };

        await DatabaseHelper.InsertMongoDocumentAsync("integration_users", testUser, "integration_testdb");

        // Act
        var collection = await DatabaseHelper.GetMongoCollectionAsync<MongoUser>("integration_users", "integration_testdb");
        var deleteResult = await collection.DeleteOneAsync(u => u.Id == testUser.Id);

        // Assert
        deleteResult.DeletedCount.Should().Be(1);
        
        var deletedUser = await collection.Find(u => u.Id == testUser.Id).FirstOrDefaultAsync();
        deletedUser.Should().BeNull();
        
        Logger.LogInformation("Successfully deleted user: {UserId}", testUser.Id);
    }

    [Fact]
    public async Task Should_Query_Users_By_Department()
    {
        // Arrange & Act
        var engineeringUsers = await DatabaseHelper.FindMongoDocumentsAsync<MongoUser>(
            "integration_users",
            Builders<MongoUser>.Filter.Eq(u => u.Department, "Engineering"),
            "integration_testdb");

        // Assert
        engineeringUsers.Should().NotBeEmpty();
        engineeringUsers.Should().HaveCountGreaterOrEqualTo(1);
        engineeringUsers.Should().OnlyContain(u => u.Department == "Engineering");
        
        Logger.LogInformation("Found {Count} engineering users", engineeringUsers.Count);
    }

    [Fact]
    public async Task Should_Query_Active_Users_Only()
    {
        // Arrange & Act
        var activeUsers = await DatabaseHelper.FindMongoDocumentsAsync<MongoUser>(
            "integration_users",
            Builders<MongoUser>.Filter.Eq(u => u.IsActive, true),
            "integration_testdb");

        // Assert
        activeUsers.Should().NotBeEmpty();
        activeUsers.Should().OnlyContain(u => u.IsActive == true);
        
        Logger.LogInformation("Found {Count} active users", activeUsers.Count);
    }

    [Fact]
    public async Task Should_Handle_Complex_MongoDB_Aggregation()
    {
        // Arrange
        var collection = await DatabaseHelper.GetMongoCollectionAsync<MongoUser>("integration_users", "integration_testdb");

        // Act - Aggregate users by department with average salary
        var pipeline = new[]
        {
            new BsonDocument("$group", new BsonDocument
            {
                { "_id", "$department" },
                { "count", new BsonDocument("$sum", 1) },
                { "avgSalary", new BsonDocument("$avg", "$salary") },
                { "totalSalary", new BsonDocument("$sum", "$salary") }
            }),
            new BsonDocument("$sort", new BsonDocument("count", -1))
        };

        var aggregationResult = await collection.Aggregate<BsonDocument>(pipeline).ToListAsync();

        // Assert
        aggregationResult.Should().NotBeEmpty();
        
        var engineeringDept = aggregationResult.FirstOrDefault(r => r["_id"].AsString == "Engineering");
        engineeringDept.Should().NotBeNull();
        engineeringDept!["count"].AsInt32.Should().BeGreaterThan(0);
        engineeringDept["avgSalary"].AsDouble.Should().BeGreaterThan(0);
        
        Logger.LogInformation("Aggregation returned {Count} departments", aggregationResult.Count);
    }

    [Fact]
    public async Task Should_Produce_And_Consume_MongoDB_Change_Events()
    {
        // Arrange
        var topicName = "mongodb-change-events";
        await WaitForKafkaTopicAsync(topicName);
        
        KafkaConsumer.Subscribe(topicName);

        // Act - Insert a new user (simulating a change event)
        var changeUser = new MongoUser
        {
            Id = "change_event_user",
            Name = "Change Event User",
            Email = "change.event@example.com",
            Age = 30,
            Department = "Engineering",
            Salary = 70000,
            CreatedAt = DateTime.UtcNow,
            UpdatedAt = DateTime.UtcNow,
            IsActive = true,
            Tags = new[] { "test", "change-event" },
            Address = new Address
            {
                Street = "456 Change St",
                City = "Event City",
                State = "EC",
                ZipCode = "54321",
                Country = "USA"
            }
        };

        await DatabaseHelper.InsertMongoDocumentAsync("integration_users", changeUser, "integration_testdb");

        // Simulate producing a change event to Kafka
        var changeEvent = new
        {
            operation = "insert",
            database = "integration_testdb",
            collection = "integration_users",
            document = changeUser,
            timestamp = DateTime.UtcNow
        };

        await KafkaProducer.ProduceJsonAsync(topicName, changeUser.Id, changeEvent);

        // Assert - Consume the change event
        var messages = await KafkaConsumer.ConsumeMessagesAsync(1, TimeSpan.FromSeconds(30));
        messages.Should().HaveCount(1);

        var consumedMessage = messages[0];
        consumedMessage.Message.Key.Should().Be(changeUser.Id);
        
        var deserializedEvent = JsonSerializer.Deserialize<JsonElement>(consumedMessage.Message.Value);
        deserializedEvent.GetProperty("operation").GetString().Should().Be("insert");
        deserializedEvent.GetProperty("collection").GetString().Should().Be("integration_users");
        
        Logger.LogInformation("Successfully produced and consumed MongoDB change event");
    }

    protected override void ConfigureServices(IServiceCollection services)
    {
        // Add any MongoDB-specific services here
        services.AddSingleton<TestDataSeeder>();
    }
}