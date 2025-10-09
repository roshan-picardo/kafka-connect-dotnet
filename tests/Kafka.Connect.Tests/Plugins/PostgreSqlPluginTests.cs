using FluentAssertions;
using Kafka.Connect.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using Xunit;
using Xunit.Abstractions;

namespace Kafka.Connect.Tests.Plugins;

[Collection("IntegrationTestCollection")]
public class PostgreSqlPluginTests : BaseIntegrationTest
{
    private readonly ITestOutputHelper _output;
    private TestDataSeeder _testDataSeeder = null!;

    public PostgreSqlPluginTests(ITestOutputHelper output)
    {
        _output = output;
    }

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        _testDataSeeder = new TestDataSeeder(DatabaseHelper, Configuration, Logger);
        
        // Wait for PostgreSQL to be ready
        await DatabaseHelper.WaitForDatabaseReadyAsync("postgresql", TimeSpan.FromMinutes(2));
        
        // Seed test data
        await _testDataSeeder.SeedPostgreSqlAsync();
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
    public async Task Should_Connect_To_PostgreSQL_Successfully()
    {
        // Arrange & Act
        using var connection = await DatabaseHelper.GetPostgresConnectionAsync();
        var result = await DatabaseHelper.ExecutePostgresScalarAsync<int>("SELECT 1");

        // Assert
        result.Should().Be(1);
        Logger.LogInformation("PostgreSQL connection test passed");
    }

    [Fact]
    public async Task Should_Read_Users_From_PostgreSQL_Table()
    {
        // Arrange & Act
        var users = await DatabaseHelper.ExecutePostgresQueryAsync(
            "SELECT id, name, email, age, department, salary, is_active FROM integration_test.integration_users ORDER BY created_at");

        // Assert
        users.Should().NotBeEmpty();
        users.Should().HaveCountGreaterOrEqualTo(2);
        
        var johnDoe = users.FirstOrDefault(u => u["email"]?.ToString() == "john.doe@example.com");
        johnDoe.Should().NotBeNull();
        johnDoe!["name"].Should().Be("John Doe");
        johnDoe["department"].Should().Be("Engineering");
        johnDoe["age"].Should().Be(30);
        
        Logger.LogInformation("Found {Count} users in PostgreSQL", users.Count);
    }

    [Fact]
    public async Task Should_Insert_New_User_Into_PostgreSQL()
    {
        // Arrange
        var newUserId = "test_user_" + Guid.NewGuid().ToString("N")[..8];
        var newUser = new
        {
            id = newUserId,
            name = "Test User",
            email = $"test.user.{Guid.NewGuid():N}@example.com",
            age = 28,
            department = "QA",
            salary = 60000m,
            created_at = DateTime.UtcNow,
            updated_at = DateTime.UtcNow,
            is_active = true
        };

        // Act
        await DatabaseHelper.ExecutePostgresSqlAsync(@"
            INSERT INTO integration_test.integration_users 
            (id, name, email, age, department, salary, created_at, updated_at, is_active)
            VALUES (@id, @name, @email, @age, @department, @salary, @created_at, @updated_at, @is_active)", newUser);

        // Assert
        var insertedUser = await DatabaseHelper.ExecutePostgresQueryAsync(
            "SELECT * FROM integration_test.integration_users WHERE id = @id", 
            new { id = newUserId });

        insertedUser.Should().HaveCount(1);
        insertedUser[0]["name"].Should().Be("Test User");
        insertedUser[0]["department"].Should().Be("QA");
        insertedUser[0]["is_active"].Should().Be(true);
        
        Logger.LogInformation("Successfully inserted and verified new user: {UserId}", newUserId);
    }

    [Fact]
    public async Task Should_Update_User_In_PostgreSQL()
    {
        // Arrange
        var existingUsers = await DatabaseHelper.ExecutePostgresQueryAsync(
            "SELECT id FROM integration_test.integration_users WHERE email = @email", 
            new { email = "john.doe@example.com" });
        
        existingUsers.Should().HaveCount(1);
        var userId = existingUsers[0]["id"]?.ToString();

        var updatedSalary = 80000m;
        var updatedDepartment = "Senior Engineering";

        // Act
        await DatabaseHelper.ExecutePostgresSqlAsync(@"
            UPDATE integration_test.integration_users 
            SET salary = @salary, department = @department, updated_at = @updated_at 
            WHERE id = @id", 
            new { 
                salary = updatedSalary, 
                department = updatedDepartment, 
                updated_at = DateTime.UtcNow, 
                id = userId 
            });

        // Assert
        var updatedUser = await DatabaseHelper.ExecutePostgresQueryAsync(
            "SELECT salary, department FROM integration_test.integration_users WHERE id = @id", 
            new { id = userId });

        updatedUser.Should().HaveCount(1);
        Convert.ToDecimal(updatedUser[0]["salary"]).Should().Be(updatedSalary);
        updatedUser[0]["department"].Should().Be(updatedDepartment);
        
        Logger.LogInformation("Successfully updated user: {UserId}", userId);
    }

    [Fact]
    public async Task Should_Delete_User_From_PostgreSQL()
    {
        // Arrange
        var testUserId = "delete_test_user";
        var testUser = new
        {
            id = testUserId,
            name = "Delete Test User",
            email = "delete.test@example.com",
            age = 25,
            department = "Test",
            salary = 50000m,
            created_at = DateTime.UtcNow,
            updated_at = DateTime.UtcNow,
            is_active = true
        };

        await DatabaseHelper.ExecutePostgresSqlAsync(@"
            INSERT INTO integration_test.integration_users 
            (id, name, email, age, department, salary, created_at, updated_at, is_active)
            VALUES (@id, @name, @email, @age, @department, @salary, @created_at, @updated_at, @is_active)", testUser);

        // Act
        var deletedRows = await DatabaseHelper.ExecutePostgresScalarAsync<long>(
            "DELETE FROM integration_test.integration_users WHERE id = @id RETURNING 1", 
            new { id = testUserId });

        // Assert
        deletedRows.Should().Be(1);
        
        var deletedUser = await DatabaseHelper.ExecutePostgresQueryAsync(
            "SELECT * FROM integration_test.integration_users WHERE id = @id", 
            new { id = testUserId });
        
        deletedUser.Should().BeEmpty();
        
        Logger.LogInformation("Successfully deleted user: {UserId}", testUserId);
    }

    [Fact]
    public async Task Should_Query_Users_By_Department()
    {
        // Arrange & Act
        var engineeringUsers = await DatabaseHelper.ExecutePostgresQueryAsync(
            "SELECT * FROM integration_test.integration_users WHERE department = @department", 
            new { department = "Engineering" });

        // Assert
        engineeringUsers.Should().NotBeEmpty();
        engineeringUsers.Should().OnlyContain(u => u["department"] != null && u["department"].ToString() == "Engineering");
        
        Logger.LogInformation("Found {Count} engineering users", engineeringUsers.Count);
    }

    [Fact]
    public async Task Should_Query_Active_Users_Only()
    {
        // Arrange & Act
        var activeUsers = await DatabaseHelper.ExecutePostgresQueryAsync(
            "SELECT * FROM integration_test.integration_users WHERE is_active = @is_active", 
            new { is_active = true });

        // Assert
        activeUsers.Should().NotBeEmpty();
        activeUsers.Should().OnlyContain(u => Convert.ToBoolean(u["is_active"]) == true);
        
        Logger.LogInformation("Found {Count} active users", activeUsers.Count);
    }

    [Fact]
    public async Task Should_Handle_Complex_PostgreSQL_Query()
    {
        // Arrange & Act - Aggregate users by department with statistics
        var departmentStats = await DatabaseHelper.ExecutePostgresQueryAsync(@"
            SELECT 
                department,
                COUNT(*) as user_count,
                AVG(salary) as avg_salary,
                MIN(salary) as min_salary,
                MAX(salary) as max_salary,
                SUM(salary) as total_salary
            FROM integration_test.integration_users 
            WHERE is_active = true
            GROUP BY department 
            ORDER BY user_count DESC");

        // Assert
        departmentStats.Should().NotBeEmpty();
        
        var engineeringDept = departmentStats.FirstOrDefault(d => d["department"]?.ToString() == "Engineering");
        if (engineeringDept != null)
        {
            Convert.ToInt64(engineeringDept["user_count"]).Should().BeGreaterThan(0);
            Convert.ToDecimal(engineeringDept["avg_salary"]).Should().BeGreaterThan(0);
        }
        
        Logger.LogInformation("Department statistics returned {Count} departments", departmentStats.Count);
    }

    [Fact]
    public async Task Should_Handle_PostgreSQL_Transactions()
    {
        // Arrange
        var testUserId1 = "transaction_user_1";
        var testUserId2 = "transaction_user_2";

        // Act & Assert - Test successful transaction
        using (var connection = await DatabaseHelper.GetPostgresConnectionAsync())
        using (var transaction = await connection.BeginTransactionAsync())
        {
            try
            {
                // Insert first user
                using (var cmd1 = connection.CreateCommand())
                {
                    cmd1.Transaction = transaction;
                    cmd1.CommandText = @"
                        INSERT INTO integration_test.integration_users 
                        (id, name, email, age, department, salary, created_at, updated_at, is_active)
                        VALUES (@id, @name, @email, @age, @department, @salary, @created_at, @updated_at, @is_active)";
                    
                    cmd1.Parameters.Add(new Npgsql.NpgsqlParameter("@id", testUserId1));
                    cmd1.Parameters.Add(new Npgsql.NpgsqlParameter("@name", "Transaction User 1"));
                    cmd1.Parameters.Add(new Npgsql.NpgsqlParameter("@email", "transaction1@example.com"));
                    cmd1.Parameters.Add(new Npgsql.NpgsqlParameter("@age", 30));
                    cmd1.Parameters.Add(new Npgsql.NpgsqlParameter("@department", "Test"));
                    cmd1.Parameters.Add(new Npgsql.NpgsqlParameter("@salary", 55000m));
                    cmd1.Parameters.Add(new Npgsql.NpgsqlParameter("@created_at", DateTime.UtcNow));
                    cmd1.Parameters.Add(new Npgsql.NpgsqlParameter("@updated_at", DateTime.UtcNow));
                    cmd1.Parameters.Add(new Npgsql.NpgsqlParameter("@is_active", true));
                    
                    await cmd1.ExecuteNonQueryAsync();
                }

                // Insert second user
                using (var cmd2 = connection.CreateCommand())
                {
                    cmd2.Transaction = transaction;
                    cmd2.CommandText = @"
                        INSERT INTO integration_test.integration_users 
                        (id, name, email, age, department, salary, created_at, updated_at, is_active)
                        VALUES (@id, @name, @email, @age, @department, @salary, @created_at, @updated_at, @is_active)";
                    
                    cmd2.Parameters.Add(new Npgsql.NpgsqlParameter("@id", testUserId2));
                    cmd2.Parameters.Add(new Npgsql.NpgsqlParameter("@name", "Transaction User 2"));
                    cmd2.Parameters.Add(new Npgsql.NpgsqlParameter("@email", "transaction2@example.com"));
                    cmd2.Parameters.Add(new Npgsql.NpgsqlParameter("@age", 25));
                    cmd2.Parameters.Add(new Npgsql.NpgsqlParameter("@department", "Test"));
                    cmd2.Parameters.Add(new Npgsql.NpgsqlParameter("@salary", 52000m));
                    cmd2.Parameters.Add(new Npgsql.NpgsqlParameter("@created_at", DateTime.UtcNow));
                    cmd2.Parameters.Add(new Npgsql.NpgsqlParameter("@updated_at", DateTime.UtcNow));
                    cmd2.Parameters.Add(new Npgsql.NpgsqlParameter("@is_active", true));
                    
                    await cmd2.ExecuteNonQueryAsync();
                }

                await transaction.CommitAsync();
            }
            catch
            {
                await transaction.RollbackAsync();
                throw;
            }
        }

        // Verify both users were inserted
        var insertedUsers = await DatabaseHelper.ExecutePostgresQueryAsync(
            "SELECT id FROM integration_test.integration_users WHERE id IN (@id1, @id2)", 
            new { id1 = testUserId1, id2 = testUserId2 });

        insertedUsers.Should().HaveCount(2);
        
        Logger.LogInformation("Successfully completed PostgreSQL transaction test");
    }

    [Fact]
    public async Task Should_Produce_And_Consume_PostgreSQL_Change_Events()
    {
        // Arrange
        var topicName = "postgresql-change-events";
        await WaitForKafkaTopicAsync(topicName);
        
        KafkaConsumer.Subscribe(topicName);

        // Act - Insert a new user (simulating a change event)
        var changeUserId = "change_event_user";
        var changeUser = new
        {
            id = changeUserId,
            name = "Change Event User",
            email = "change.event@example.com",
            age = 30,
            department = "Engineering",
            salary = 70000m,
            created_at = DateTime.UtcNow,
            updated_at = DateTime.UtcNow,
            is_active = true
        };

        await DatabaseHelper.ExecutePostgresSqlAsync(@"
            INSERT INTO integration_test.integration_users 
            (id, name, email, age, department, salary, created_at, updated_at, is_active)
            VALUES (@id, @name, @email, @age, @department, @salary, @created_at, @updated_at, @is_active)", changeUser);

        // Simulate producing a change event to Kafka
        var changeEvent = new
        {
            operation = "insert",
            schema = "integration_test",
            table = "integration_users",
            data = changeUser,
            timestamp = DateTime.UtcNow
        };

        await KafkaProducer.ProduceJsonAsync(topicName, changeUserId, changeEvent);

        // Assert - Consume the change event
        var messages = await KafkaConsumer.ConsumeMessagesAsync(1, TimeSpan.FromSeconds(30));
        messages.Should().HaveCount(1);

        var consumedMessage = messages[0];
        consumedMessage.Message.Key.Should().Be(changeUserId);
        
        var deserializedEvent = JsonSerializer.Deserialize<JsonElement>(consumedMessage.Message.Value);
        deserializedEvent.GetProperty("operation").GetString().Should().Be("insert");
        deserializedEvent.GetProperty("table").GetString().Should().Be("integration_users");
        
        Logger.LogInformation("Successfully produced and consumed PostgreSQL change event");
    }

    protected override void ConfigureServices(IServiceCollection services)
    {
        // Add any PostgreSQL-specific services here
        services.AddSingleton<TestDataSeeder>();
    }
}