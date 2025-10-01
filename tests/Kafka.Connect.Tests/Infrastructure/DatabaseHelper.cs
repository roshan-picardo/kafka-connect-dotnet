using MongoDB.Driver;
using Npgsql;
using MySqlConnector;
using Microsoft.Data.SqlClient;
using MongoDB.Bson;
using System.Data;

namespace Kafka.Connect.Tests.Infrastructure;

public static class DatabaseHelper
{
    public static class MongoDB
    {
        public static async Task InitializeAsync(string connectionString, string databaseName)
        {
            var client = new MongoClient(connectionString);
            var database = client.GetDatabase(databaseName);

            // Drop existing collections if they exist
            await DropCollectionIfExistsAsync(database, "test_customers");
            await DropCollectionIfExistsAsync(database, "test_sink_customers");
            await DropCollectionIfExistsAsync(database, "connect_audit_logs");

            // Create collections
            await database.CreateCollectionAsync("test_customers");
            await database.CreateCollectionAsync("test_sink_customers");
            await database.CreateCollectionAsync("connect_audit_logs");

            // Insert test data
            var testCustomers = database.GetCollection<BsonDocument>("test_customers");
            var testData = new[]
            {
                new BsonDocument
                {
                    ["_id"] = ObjectId.GenerateNewId(),
                    ["id"] = 1,
                    ["name"] = "John Doe",
                    ["email"] = "john.doe@example.com",
                    ["age"] = 30,
                    ["created_at"] = DateTime.UtcNow,
                    ["updated_at"] = DateTime.UtcNow
                },
                new BsonDocument
                {
                    ["_id"] = ObjectId.GenerateNewId(),
                    ["id"] = 2,
                    ["name"] = "Jane Smith",
                    ["email"] = "jane.smith@example.com",
                    ["age"] = 25,
                    ["created_at"] = DateTime.UtcNow,
                    ["updated_at"] = DateTime.UtcNow
                },
                new BsonDocument
                {
                    ["_id"] = ObjectId.GenerateNewId(),
                    ["id"] = 3,
                    ["name"] = "Bob Johnson",
                    ["email"] = "bob.johnson@example.com",
                    ["age"] = 35,
                    ["created_at"] = DateTime.UtcNow,
                    ["updated_at"] = DateTime.UtcNow
                }
            };

            await testCustomers.InsertManyAsync(testData);

            // Create indexes
            var indexKeysDefinition = Builders<BsonDocument>.IndexKeys.Ascending("id");
            var indexOptions = new CreateIndexOptions { Unique = true };
            await testCustomers.Indexes.CreateOneAsync(new CreateIndexModel<BsonDocument>(indexKeysDefinition, indexOptions));

            var sinkCustomers = database.GetCollection<BsonDocument>("test_sink_customers");
            await sinkCustomers.Indexes.CreateOneAsync(new CreateIndexModel<BsonDocument>(indexKeysDefinition, indexOptions));

            var auditLogs = database.GetCollection<BsonDocument>("connect_audit_logs");
            var timestampIndex = Builders<BsonDocument>.IndexKeys.Ascending("timestamp");
            await auditLogs.Indexes.CreateOneAsync(new CreateIndexModel<BsonDocument>(timestampIndex));
        }

        private static async Task DropCollectionIfExistsAsync(IMongoDatabase database, string collectionName)
        {
            var collections = await database.ListCollectionNamesAsync();
            var collectionList = await collections.ToListAsync();
            if (collectionList.Contains(collectionName))
            {
                await database.DropCollectionAsync(collectionName);
            }
        }

        public static async Task<int> GetCustomerCountAsync(string connectionString, string databaseName, string collectionName)
        {
            var client = new MongoClient(connectionString);
            var database = client.GetDatabase(databaseName);
            var collection = database.GetCollection<BsonDocument>(collectionName);
            return (int)await collection.CountDocumentsAsync(new BsonDocument());
        }

        public static async Task InsertCustomerAsync(string connectionString, string databaseName, int id, string name, string email, int age)
        {
            var client = new MongoClient(connectionString);
            var database = client.GetDatabase(databaseName);
            var collection = database.GetCollection<BsonDocument>("test_customers");

            var document = new BsonDocument
            {
                ["_id"] = ObjectId.GenerateNewId(),
                ["id"] = id,
                ["name"] = name,
                ["email"] = email,
                ["age"] = age,
                ["created_at"] = DateTime.UtcNow,
                ["updated_at"] = DateTime.UtcNow
            };

            await collection.InsertOneAsync(document);
        }

        public static async Task UpdateCustomerAsync(string connectionString, string databaseName, int id, string name, string email, int age)
        {
            var client = new MongoClient(connectionString);
            var database = client.GetDatabase(databaseName);
            var collection = database.GetCollection<BsonDocument>("test_customers");

            var filter = Builders<BsonDocument>.Filter.Eq("id", id);
            var update = Builders<BsonDocument>.Update
                .Set("name", name)
                .Set("email", email)
                .Set("age", age)
                .Set("updated_at", DateTime.UtcNow);

            await collection.UpdateOneAsync(filter, update);
        }

        public static async Task DeleteCustomerAsync(string connectionString, string databaseName, int id)
        {
            var client = new MongoClient(connectionString);
            var database = client.GetDatabase(databaseName);
            var collection = database.GetCollection<BsonDocument>("test_customers");

            var filter = Builders<BsonDocument>.Filter.Eq("id", id);
            await collection.DeleteOneAsync(filter);
        }
    }

    public static class PostgreSQL
    {
        public static async Task InitializeAsync(string connectionString)
        {
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();

            // Drop and create tables
            var sql = @"
                DROP TABLE IF EXISTS test_customers CASCADE;
                DROP TABLE IF EXISTS test_sink_customers CASCADE;
                DROP TABLE IF EXISTS connect_audit_logs CASCADE;

                CREATE TABLE test_customers (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    email VARCHAR(255) UNIQUE NOT NULL,
                    age INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE test_sink_customers (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    email VARCHAR(255) UNIQUE NOT NULL,
                    age INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE connect_audit_logs (
                    id SERIAL PRIMARY KEY,
                    connector_name VARCHAR(255),
                    command_name VARCHAR(255),
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    data JSONB
                );

                INSERT INTO test_customers (id, name, email, age) VALUES 
                    (1, 'John Doe', 'john.doe@example.com', 30),
                    (2, 'Jane Smith', 'jane.smith@example.com', 25),
                    (3, 'Bob Johnson', 'bob.johnson@example.com', 35);

                CREATE INDEX idx_audit_logs_timestamp ON connect_audit_logs(timestamp);
            ";

            await using var command = new NpgsqlCommand(sql, connection);
            await command.ExecuteNonQueryAsync();
        }

        public static async Task<int> GetCustomerCountAsync(string connectionString, string tableName)
        {
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();

            await using var command = new NpgsqlCommand($"SELECT COUNT(*) FROM {tableName}", connection);
            return Convert.ToInt32(await command.ExecuteScalarAsync());
        }

        public static async Task InsertCustomerAsync(string connectionString, int id, string name, string email, int age)
        {
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();

            var sql = "INSERT INTO test_customers (id, name, email, age) VALUES (@id, @name, @email, @age)";
            await using var command = new NpgsqlCommand(sql, connection);
            command.Parameters.AddWithValue("@id", id);
            command.Parameters.AddWithValue("@name", name);
            command.Parameters.AddWithValue("@email", email);
            command.Parameters.AddWithValue("@age", age);

            await command.ExecuteNonQueryAsync();
        }

        public static async Task UpdateCustomerAsync(string connectionString, int id, string name, string email, int age)
        {
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();

            var sql = "UPDATE test_customers SET name = @name, email = @email, age = @age, updated_at = CURRENT_TIMESTAMP WHERE id = @id";
            await using var command = new NpgsqlCommand(sql, connection);
            command.Parameters.AddWithValue("@id", id);
            command.Parameters.AddWithValue("@name", name);
            command.Parameters.AddWithValue("@email", email);
            command.Parameters.AddWithValue("@age", age);

            await command.ExecuteNonQueryAsync();
        }

        public static async Task DeleteCustomerAsync(string connectionString, int id)
        {
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();

            var sql = "DELETE FROM test_customers WHERE id = @id";
            await using var command = new NpgsqlCommand(sql, connection);
            command.Parameters.AddWithValue("@id", id);

            await command.ExecuteNonQueryAsync();
        }
    }

    public static class MySQL
    {
        public static async Task InitializeAsync(string connectionString)
        {
            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync();

            var sql = @"
                DROP TABLE IF EXISTS test_customers;
                DROP TABLE IF EXISTS test_sink_customers;
                DROP TABLE IF EXISTS connect_audit_logs;

                CREATE TABLE test_customers (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    email VARCHAR(255) UNIQUE NOT NULL,
                    age INT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                );

                CREATE TABLE test_sink_customers (
                    id INT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    email VARCHAR(255) UNIQUE NOT NULL,
                    age INT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                );

                CREATE TABLE connect_audit_logs (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    connector_name VARCHAR(255),
                    command_name VARCHAR(255),
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    data JSON
                );

                INSERT INTO test_customers (id, name, email, age) VALUES 
                    (1, 'John Doe', 'john.doe@example.com', 30),
                    (2, 'Jane Smith', 'jane.smith@example.com', 25),
                    (3, 'Bob Johnson', 'bob.johnson@example.com', 35);

                CREATE INDEX idx_audit_logs_timestamp ON connect_audit_logs(timestamp);
            ";

            await using var command = new MySqlCommand(sql, connection);
            await command.ExecuteNonQueryAsync();
        }

        public static async Task<int> GetCustomerCountAsync(string connectionString, string tableName)
        {
            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync();

            await using var command = new MySqlCommand($"SELECT COUNT(*) FROM {tableName}", connection);
            return Convert.ToInt32(await command.ExecuteScalarAsync());
        }

        public static async Task InsertCustomerAsync(string connectionString, int id, string name, string email, int age)
        {
            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync();

            var sql = "INSERT INTO test_customers (id, name, email, age) VALUES (@id, @name, @email, @age)";
            await using var command = new MySqlCommand(sql, connection);
            command.Parameters.AddWithValue("@id", id);
            command.Parameters.AddWithValue("@name", name);
            command.Parameters.AddWithValue("@email", email);
            command.Parameters.AddWithValue("@age", age);

            await command.ExecuteNonQueryAsync();
        }

        public static async Task UpdateCustomerAsync(string connectionString, int id, string name, string email, int age)
        {
            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync();

            var sql = "UPDATE test_customers SET name = @name, email = @email, age = @age WHERE id = @id";
            await using var command = new MySqlCommand(sql, connection);
            command.Parameters.AddWithValue("@id", id);
            command.Parameters.AddWithValue("@name", name);
            command.Parameters.AddWithValue("@email", email);
            command.Parameters.AddWithValue("@age", age);

            await command.ExecuteNonQueryAsync();
        }

        public static async Task DeleteCustomerAsync(string connectionString, int id)
        {
            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync();

            var sql = "DELETE FROM test_customers WHERE id = @id";
            await using var command = new MySqlCommand(sql, connection);
            command.Parameters.AddWithValue("@id", id);

            await command.ExecuteNonQueryAsync();
        }
    }

    public static class SqlServer
    {
        public static async Task InitializeAsync(string connectionString)
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            var sql = @"
                IF OBJECT_ID('test_customers', 'U') IS NOT NULL DROP TABLE test_customers;
                IF OBJECT_ID('test_sink_customers', 'U') IS NOT NULL DROP TABLE test_sink_customers;
                IF OBJECT_ID('connect_audit_logs', 'U') IS NOT NULL DROP TABLE connect_audit_logs;

                CREATE TABLE test_customers (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name NVARCHAR(255) NOT NULL,
                    email NVARCHAR(255) UNIQUE NOT NULL,
                    age INT,
                    created_at DATETIME2 DEFAULT GETUTCDATE(),
                    updated_at DATETIME2 DEFAULT GETUTCDATE()
                );

                CREATE TABLE test_sink_customers (
                    id INT PRIMARY KEY,
                    name NVARCHAR(255) NOT NULL,
                    email NVARCHAR(255) UNIQUE NOT NULL,
                    age INT,
                    created_at DATETIME2 DEFAULT GETUTCDATE(),
                    updated_at DATETIME2 DEFAULT GETUTCDATE()
                );

                CREATE TABLE connect_audit_logs (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    connector_name NVARCHAR(255),
                    command_name NVARCHAR(255),
                    timestamp DATETIME2 DEFAULT GETUTCDATE(),
                    data NVARCHAR(MAX)
                );

                SET IDENTITY_INSERT test_customers ON;
                INSERT INTO test_customers (id, name, email, age) VALUES 
                    (1, 'John Doe', 'john.doe@example.com', 30),
                    (2, 'Jane Smith', 'jane.smith@example.com', 25),
                    (3, 'Bob Johnson', 'bob.johnson@example.com', 35);
                SET IDENTITY_INSERT test_customers OFF;

                CREATE INDEX idx_audit_logs_timestamp ON connect_audit_logs(timestamp);
            ";

            await using var command = new SqlCommand(sql, connection);
            await command.ExecuteNonQueryAsync();
        }

        public static async Task<int> GetCustomerCountAsync(string connectionString, string tableName)
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            await using var command = new SqlCommand($"SELECT COUNT(*) FROM {tableName}", connection);
            return Convert.ToInt32(await command.ExecuteScalarAsync());
        }

        public static async Task InsertCustomerAsync(string connectionString, int id, string name, string email, int age)
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            var sql = "SET IDENTITY_INSERT test_customers ON; INSERT INTO test_customers (id, name, email, age) VALUES (@id, @name, @email, @age); SET IDENTITY_INSERT test_customers OFF;";
            await using var command = new SqlCommand(sql, connection);
            command.Parameters.AddWithValue("@id", id);
            command.Parameters.AddWithValue("@name", name);
            command.Parameters.AddWithValue("@email", email);
            command.Parameters.AddWithValue("@age", age);

            await command.ExecuteNonQueryAsync();
        }

        public static async Task UpdateCustomerAsync(string connectionString, int id, string name, string email, int age)
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            var sql = "UPDATE test_customers SET name = @name, email = @email, age = @age, updated_at = GETUTCDATE() WHERE id = @id";
            await using var command = new SqlCommand(sql, connection);
            command.Parameters.AddWithValue("@id", id);
            command.Parameters.AddWithValue("@name", name);
            command.Parameters.AddWithValue("@email", email);
            command.Parameters.AddWithValue("@age", age);

            await command.ExecuteNonQueryAsync();
        }

        public static async Task DeleteCustomerAsync(string connectionString, int id)
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            var sql = "DELETE FROM test_customers WHERE id = @id";
            await using var command = new SqlCommand(sql, connection);
            command.Parameters.AddWithValue("@id", id);

            await command.ExecuteNonQueryAsync();
        }
    }
}