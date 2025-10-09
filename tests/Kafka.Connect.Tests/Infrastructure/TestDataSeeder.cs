using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using System.Text.Json;

namespace Kafka.Connect.Tests.Infrastructure;

public class TestDataSeeder
{
    private readonly DatabaseHelper _databaseHelper;
    private readonly IConfiguration _configuration;
    private readonly ILogger _logger;

    public TestDataSeeder(DatabaseHelper databaseHelper, IConfiguration configuration, ILogger logger)
    {
        _databaseHelper = databaseHelper;
        _configuration = configuration;
        _logger = logger;
    }

    public async Task SeedAllDatabasesAsync()
    {
        await SeedMongoDbAsync();
        await SeedPostgreSqlAsync();
        await SeedMySqlAsync();
    }

    public async Task SeedMongoDbAsync()
    {
        try
        {
            _logger.LogInformation("Seeding MongoDB test data...");
            
            var database = _databaseHelper.GetMongoDatabase("integration_testdb");
            
            // Seed users
            await SeedMongoUsersAsync(database);
            
            // Seed orders
            await SeedMongoOrdersAsync(database);
            
            // Seed products
            await SeedMongoProductsAsync(database);
            
            _logger.LogInformation("MongoDB test data seeded successfully");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to seed MongoDB test data");
            throw;
        }
    }

    private async Task SeedMongoUsersAsync(IMongoDatabase database)
    {
        var collection = database.GetCollection<MongoUser>("integration_users");
        
        // Clear existing data
        await collection.DeleteManyAsync(Builders<MongoUser>.Filter.Empty);
        
        var users = new[]
        {
            new MongoUser
            {
                Id = "user1",
                Name = "John Doe",
                Email = "john.doe@example.com",
                Age = 30,
                Department = "Engineering",
                Salary = 75000,
                CreatedAt = DateTime.Parse("2024-01-01T00:00:00Z"),
                UpdatedAt = DateTime.Parse("2024-01-01T00:00:00Z"),
                IsActive = true,
                Tags = new[] { "developer", "senior", "fullstack" },
                Address = new Address
                {
                    Street = "123 Main St",
                    City = "San Francisco",
                    State = "CA",
                    ZipCode = "94105",
                    Country = "USA"
                }
            },
            new MongoUser
            {
                Id = "user2",
                Name = "Jane Smith",
                Email = "jane.smith@example.com",
                Age = 25,
                Department = "Marketing",
                Salary = 65000,
                CreatedAt = DateTime.Parse("2024-01-02T00:00:00Z"),
                UpdatedAt = DateTime.Parse("2024-01-02T00:00:00Z"),
                IsActive = true,
                Tags = new[] { "marketing", "junior", "digital" },
                Address = new Address
                {
                    Street = "456 Oak Ave",
                    City = "New York",
                    State = "NY",
                    ZipCode = "10001",
                    Country = "USA"
                }
            },
            new MongoUser
            {
                Id = "user3",
                Name = "Bob Johnson",
                Email = "bob.johnson@example.com",
                Age = 35,
                Department = "Engineering",
                Salary = 85000,
                CreatedAt = DateTime.Parse("2024-01-03T00:00:00Z"),
                UpdatedAt = DateTime.Parse("2024-01-03T00:00:00Z"),
                IsActive = false,
                Tags = new[] { "developer", "senior", "backend" },
                Address = new Address
                {
                    Street = "789 Pine St",
                    City = "Seattle",
                    State = "WA",
                    ZipCode = "98101",
                    Country = "USA"
                }
            }
        };

        await collection.InsertManyAsync(users);
        
        // Create indexes
        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<MongoUser>(Builders<MongoUser>.IndexKeys.Ascending(u => u.Email),
            new CreateIndexOptions { Unique = true }));
        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<MongoUser>(Builders<MongoUser>.IndexKeys.Ascending(u => u.Department)));
        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<MongoUser>(Builders<MongoUser>.IndexKeys.Ascending(u => u.CreatedAt)));
        
        _logger.LogDebug("Seeded {Count} users in MongoDB", users.Length);
    }

    private async Task SeedMongoOrdersAsync(IMongoDatabase database)
    {
        var collection = database.GetCollection<MongoOrder>("integration_orders");
        
        // Clear existing data
        await collection.DeleteManyAsync(Builders<MongoOrder>.Filter.Empty);
        
        var orders = new[]
        {
            new MongoOrder
            {
                Id = "order1",
                UserId = "user1",
                ProductName = "Laptop",
                ProductId = "prod1",
                Quantity = 1,
                Price = 1299.99m,
                Currency = "USD",
                OrderDate = DateTime.Parse("2024-01-15T10:30:00Z"),
                Status = "completed",
                ShippingAddress = new Address
                {
                    Street = "123 Main St",
                    City = "San Francisco",
                    State = "CA",
                    ZipCode = "94105",
                    Country = "USA"
                },
                Items = new[]
                {
                    new OrderItem
                    {
                        ProductId = "prod1",
                        Name = "Laptop",
                        Quantity = 1,
                        UnitPrice = 1299.99m
                    }
                }
            },
            new MongoOrder
            {
                Id = "order2",
                UserId = "user2",
                ProductName = "Mouse",
                ProductId = "prod2",
                Quantity = 2,
                Price = 49.98m,
                Currency = "USD",
                OrderDate = DateTime.Parse("2024-01-16T14:20:00Z"),
                Status = "pending",
                ShippingAddress = new Address
                {
                    Street = "456 Oak Ave",
                    City = "New York",
                    State = "NY",
                    ZipCode = "10001",
                    Country = "USA"
                },
                Items = new[]
                {
                    new OrderItem
                    {
                        ProductId = "prod2",
                        Name = "Mouse",
                        Quantity = 2,
                        UnitPrice = 24.99m
                    }
                }
            }
        };

        await collection.InsertManyAsync(orders);
        
        // Create indexes
        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<MongoOrder>(Builders<MongoOrder>.IndexKeys.Ascending(o => o.UserId)));
        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<MongoOrder>(Builders<MongoOrder>.IndexKeys.Ascending(o => o.OrderDate)));
        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<MongoOrder>(Builders<MongoOrder>.IndexKeys.Ascending(o => o.Status)));
        
        _logger.LogDebug("Seeded {Count} orders in MongoDB", orders.Length);
    }

    private async Task SeedMongoProductsAsync(IMongoDatabase database)
    {
        var collection = database.GetCollection<MongoProduct>("integration_products");
        
        // Clear existing data
        await collection.DeleteManyAsync(Builders<MongoProduct>.Filter.Empty);
        
        var products = new[]
        {
            new MongoProduct
            {
                Id = "prod1",
                Name = "Laptop",
                Description = "High-performance laptop for developers",
                Category = "Electronics",
                Price = 1299.99m,
                Currency = "USD",
                InStock = true,
                StockQuantity = 50,
                CreatedAt = DateTime.Parse("2024-01-01T00:00:00Z"),
                UpdatedAt = DateTime.Parse("2024-01-01T00:00:00Z"),
                Specifications = new Dictionary<string, object>
                {
                    ["cpu"] = "Intel i7",
                    ["ram"] = "16GB",
                    ["storage"] = "512GB SSD",
                    ["screen"] = "15.6 inch"
                },
                Tags = new[] { "laptop", "computer", "electronics" }
            },
            new MongoProduct
            {
                Id = "prod2",
                Name = "Mouse",
                Description = "Wireless optical mouse",
                Category = "Accessories",
                Price = 24.99m,
                Currency = "USD",
                InStock = true,
                StockQuantity = 200,
                CreatedAt = DateTime.Parse("2024-01-01T00:00:00Z"),
                UpdatedAt = DateTime.Parse("2024-01-01T00:00:00Z"),
                Specifications = new Dictionary<string, object>
                {
                    ["type"] = "Wireless",
                    ["dpi"] = "1600",
                    ["buttons"] = "3"
                },
                Tags = new[] { "mouse", "wireless", "accessories" }
            }
        };

        await collection.InsertManyAsync(products);
        
        // Create indexes
        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<MongoProduct>(Builders<MongoProduct>.IndexKeys.Ascending(p => p.Category)));
        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<MongoProduct>(Builders<MongoProduct>.IndexKeys.Ascending(p => p.Price)));
        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<MongoProduct>(Builders<MongoProduct>.IndexKeys.Ascending(p => p.InStock)));
        
        _logger.LogDebug("Seeded {Count} products in MongoDB", products.Length);
    }

    public async Task SeedPostgreSqlAsync()
    {
        try
        {
            _logger.LogInformation("Seeding PostgreSQL test data...");
            
            // Create schema and tables
            await _databaseHelper.ExecutePostgresSqlAsync("CREATE SCHEMA IF NOT EXISTS integration_test;");
            
            await _databaseHelper.ExecutePostgresSqlAsync(@"
                CREATE TABLE IF NOT EXISTS integration_test.integration_users (
                    id VARCHAR(50) PRIMARY KEY,
                    name VARCHAR(100),
                    email VARCHAR(100) UNIQUE,
                    age INTEGER,
                    department VARCHAR(50),
                    salary DECIMAL(10,2),
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    is_active BOOLEAN
                );");
            
            // Clear existing data
            await _databaseHelper.ExecutePostgresSqlAsync("DELETE FROM integration_test.integration_users;");
            
            // Insert sample data
            var users = new[]
            {
                new { id = "user1", name = "John Doe", email = "john.doe@example.com", age = 30, department = "Engineering", salary = 75000m, created_at = DateTime.Parse("2024-01-01T00:00:00Z"), updated_at = DateTime.Parse("2024-01-01T00:00:00Z"), is_active = true },
                new { id = "user2", name = "Jane Smith", email = "jane.smith@example.com", age = 25, department = "Marketing", salary = 65000m, created_at = DateTime.Parse("2024-01-02T00:00:00Z"), updated_at = DateTime.Parse("2024-01-02T00:00:00Z"), is_active = true }
            };
            
            foreach (var user in users)
            {
                await _databaseHelper.ExecutePostgresSqlAsync(@"
                    INSERT INTO integration_test.integration_users 
                    (id, name, email, age, department, salary, created_at, updated_at, is_active)
                    VALUES (@id, @name, @email, @age, @department, @salary, @created_at, @updated_at, @is_active);", user);
            }
            
            _logger.LogInformation("PostgreSQL test data seeded successfully");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to seed PostgreSQL test data");
            throw;
        }
    }

    public async Task SeedMySqlAsync()
    {
        try
        {
            _logger.LogInformation("Seeding MySQL test data...");
            
            // Create tables
            await _databaseHelper.ExecuteMySqlAsync(@"
                CREATE TABLE IF NOT EXISTS integration_users (
                    id VARCHAR(50) PRIMARY KEY,
                    name VARCHAR(100),
                    email VARCHAR(100) UNIQUE,
                    age INTEGER,
                    department VARCHAR(50),
                    salary DECIMAL(10,2),
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    is_active BOOLEAN
                );");
            
            // Clear existing data
            await _databaseHelper.ExecuteMySqlAsync("DELETE FROM integration_users;");
            
            // Insert sample data
            var users = new[]
            {
                new { id = "user1", name = "John Doe", email = "john.doe@example.com", age = 30, department = "Engineering", salary = 75000m, created_at = DateTime.Parse("2024-01-01T00:00:00Z"), updated_at = DateTime.Parse("2024-01-01T00:00:00Z"), is_active = true },
                new { id = "user2", name = "Jane Smith", email = "jane.smith@example.com", age = 25, department = "Marketing", salary = 65000m, created_at = DateTime.Parse("2024-01-02T00:00:00Z"), updated_at = DateTime.Parse("2024-01-02T00:00:00Z"), is_active = true }
            };
            
            foreach (var user in users)
            {
                await _databaseHelper.ExecuteMySqlAsync(@"
                    INSERT INTO integration_users 
                    (id, name, email, age, department, salary, created_at, updated_at, is_active)
                    VALUES (@id, @name, @email, @age, @department, @salary, @created_at, @updated_at, @is_active);", user);
            }
            
            _logger.LogInformation("MySQL test data seeded successfully");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to seed MySQL test data");
            throw;
        }
    }

    public async Task CleanupAllDatabasesAsync()
    {
        try
        {
            _logger.LogInformation("Cleaning up test databases...");
            
            // MongoDB cleanup
            var mongoDb = _databaseHelper.GetMongoDatabase("integration_testdb");
            await mongoDb.DropCollectionAsync("integration_users");
            await mongoDb.DropCollectionAsync("integration_orders");
            await mongoDb.DropCollectionAsync("integration_products");
            
            // PostgreSQL cleanup
            await _databaseHelper.ExecutePostgresSqlAsync("DROP SCHEMA IF EXISTS integration_test CASCADE;");
            
            // MySQL cleanup
            await _databaseHelper.ExecuteMySqlAsync("DROP TABLE IF EXISTS integration_users;");
            
            _logger.LogInformation("Test databases cleaned up successfully");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to cleanup test databases");
        }
    }
}

// Data models for MongoDB
public class MongoUser
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public int Age { get; set; }
    public string Department { get; set; } = string.Empty;
    public decimal Salary { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime UpdatedAt { get; set; }
    public bool IsActive { get; set; }
    public string[] Tags { get; set; } = Array.Empty<string>();
    public Address Address { get; set; } = new();
}

public class MongoOrder
{
    public string Id { get; set; } = string.Empty;
    public string UserId { get; set; } = string.Empty;
    public string ProductName { get; set; } = string.Empty;
    public string ProductId { get; set; } = string.Empty;
    public int Quantity { get; set; }
    public decimal Price { get; set; }
    public string Currency { get; set; } = string.Empty;
    public DateTime OrderDate { get; set; }
    public string Status { get; set; } = string.Empty;
    public Address ShippingAddress { get; set; } = new();
    public OrderItem[] Items { get; set; } = Array.Empty<OrderItem>();
}

public class MongoProduct
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public string Currency { get; set; } = string.Empty;
    public bool InStock { get; set; }
    public int StockQuantity { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime UpdatedAt { get; set; }
    public Dictionary<string, object> Specifications { get; set; } = new();
    public string[] Tags { get; set; } = Array.Empty<string>();
}

public class Address
{
    public string Street { get; set; } = string.Empty;
    public string City { get; set; } = string.Empty;
    public string State { get; set; } = string.Empty;
    public string ZipCode { get; set; } = string.Empty;
    public string Country { get; set; } = string.Empty;
}

public class OrderItem
{
    public string ProductId { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public int Quantity { get; set; }
    public decimal UnitPrice { get; set; }
}