using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using Npgsql;
using MySqlConnector;
using System.Data;

namespace Kafka.Connect.Tests.Infrastructure;

public class DatabaseHelper
{
    private readonly IConfiguration _configuration;
    private readonly ILogger _logger;

    public DatabaseHelper(IConfiguration configuration, ILogger logger)
    {
        _configuration = configuration;
        _logger = logger;
    }

    #region MongoDB Operations

    public IMongoDatabase GetMongoDatabase(string? databaseName = null)
    {
        var connectionString = _configuration.GetConnectionString("MongoDB") 
            ?? _configuration["MongoDB:ConnectionString"];
        
        if (string.IsNullOrEmpty(connectionString))
            throw new InvalidOperationException("MongoDB connection string not found");

        var client = new MongoClient(connectionString);
        return client.GetDatabase(databaseName ?? "testdb");
    }

    public async Task<IMongoCollection<T>> GetMongoCollectionAsync<T>(string collectionName, string? databaseName = null)
    {
        var database = GetMongoDatabase(databaseName);
        return database.GetCollection<T>(collectionName);
    }

    public async Task InsertMongoDocumentAsync<T>(string collectionName, T document, string? databaseName = null)
    {
        var collection = await GetMongoCollectionAsync<T>(collectionName, databaseName);
        await collection.InsertOneAsync(document);
        _logger.LogDebug("Inserted document into MongoDB collection {Collection}", collectionName);
    }

    public async Task InsertMongoDocumentsAsync<T>(string collectionName, IEnumerable<T> documents, string? databaseName = null)
    {
        var collection = await GetMongoCollectionAsync<T>(collectionName, databaseName);
        await collection.InsertManyAsync(documents);
        _logger.LogDebug("Inserted {Count} documents into MongoDB collection {Collection}", 
            documents.Count(), collectionName);
    }

    public async Task<List<T>> FindMongoDocumentsAsync<T>(string collectionName, FilterDefinition<T>? filter = null, string? databaseName = null)
    {
        var collection = await GetMongoCollectionAsync<T>(collectionName, databaseName);
        filter ??= Builders<T>.Filter.Empty;
        
        var documents = await collection.Find(filter).ToListAsync();
        _logger.LogDebug("Found {Count} documents in MongoDB collection {Collection}", 
            documents.Count, collectionName);
        
        return documents;
    }

    public async Task ClearMongoCollectionAsync<T>(string collectionName, string? databaseName = null)
    {
        var collection = await GetMongoCollectionAsync<T>(collectionName, databaseName);
        await collection.DeleteManyAsync(Builders<T>.Filter.Empty);
        _logger.LogDebug("Cleared MongoDB collection {Collection}", collectionName);
    }

    #endregion

    #region PostgreSQL Operations

    public async Task<NpgsqlConnection> GetPostgresConnectionAsync()
    {
        var connectionString = _configuration.GetConnectionString("PostgreSQL") 
            ?? _configuration["PostgreSQL:ConnectionString"];
        
        if (string.IsNullOrEmpty(connectionString))
            throw new InvalidOperationException("PostgreSQL connection string not found");

        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        return connection;
    }

    public async Task ExecutePostgresSqlAsync(string sql, object? parameters = null)
    {
        using var connection = await GetPostgresConnectionAsync();
        using var command = new NpgsqlCommand(sql, connection);
        
        if (parameters != null)
        {
            AddParametersToCommand(command, parameters);
        }
        
        await command.ExecuteNonQueryAsync();
        _logger.LogDebug("Executed PostgreSQL command: {Sql}", sql);
    }

    public async Task<T?> ExecutePostgresScalarAsync<T>(string sql, object? parameters = null)
    {
        using var connection = await GetPostgresConnectionAsync();
        using var command = new NpgsqlCommand(sql, connection);
        
        if (parameters != null)
        {
            AddParametersToCommand(command, parameters);
        }
        
        var result = await command.ExecuteScalarAsync();
        _logger.LogDebug("Executed PostgreSQL scalar command: {Sql}", sql);
        
        return result is T value ? value : default;
    }

    public async Task<List<Dictionary<string, object?>>> ExecutePostgresQueryAsync(string sql, object? parameters = null)
    {
        using var connection = await GetPostgresConnectionAsync();
        using var command = new NpgsqlCommand(sql, connection);
        
        if (parameters != null)
        {
            AddParametersToCommand(command, parameters);
        }
        
        using var reader = await command.ExecuteReaderAsync();
        var results = new List<Dictionary<string, object?>>();
        
        while (await reader.ReadAsync())
        {
            var row = new Dictionary<string, object?>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }
            results.Add(row);
        }
        
        _logger.LogDebug("Executed PostgreSQL query: {Sql}, returned {Count} rows", sql, results.Count);
        return results;
    }

    #endregion

    #region MySQL Operations

    public async Task<MySqlConnection> GetMySqlConnectionAsync()
    {
        var connectionString = _configuration.GetConnectionString("MySQL") 
            ?? _configuration["MySQL:ConnectionString"];
        
        if (string.IsNullOrEmpty(connectionString))
            throw new InvalidOperationException("MySQL connection string not found");

        var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync();
        return connection;
    }

    public async Task ExecuteMySqlAsync(string sql, object? parameters = null)
    {
        using var connection = await GetMySqlConnectionAsync();
        using var command = new MySqlCommand(sql, connection);
        
        if (parameters != null)
        {
            AddParametersToCommand(command, parameters);
        }
        
        await command.ExecuteNonQueryAsync();
        _logger.LogDebug("Executed MySQL command: {Sql}", sql);
    }

    public async Task<T?> ExecuteMySqlScalarAsync<T>(string sql, object? parameters = null)
    {
        using var connection = await GetMySqlConnectionAsync();
        using var command = new MySqlCommand(sql, connection);
        
        if (parameters != null)
        {
            AddParametersToCommand(command, parameters);
        }
        
        var result = await command.ExecuteScalarAsync();
        _logger.LogDebug("Executed MySQL scalar command: {Sql}", sql);
        
        return result is T value ? value : default;
    }

    public async Task<List<Dictionary<string, object?>>> ExecuteMySqlQueryAsync(string sql, object? parameters = null)
    {
        using var connection = await GetMySqlConnectionAsync();
        using var command = new MySqlCommand(sql, connection);
        
        if (parameters != null)
        {
            AddParametersToCommand(command, parameters);
        }
        
        using var reader = await command.ExecuteReaderAsync();
        var results = new List<Dictionary<string, object?>>();
        
        while (await reader.ReadAsync())
        {
            var row = new Dictionary<string, object?>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }
            results.Add(row);
        }
        
        _logger.LogDebug("Executed MySQL query: {Sql}, returned {Count} rows", sql, results.Count);
        return results;
    }

    #endregion

    #region Helper Methods

    private static void AddParametersToCommand(IDbCommand command, object parameters)
    {
        var properties = parameters.GetType().GetProperties();
        
        foreach (var property in properties)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = $"@{property.Name}";
            parameter.Value = property.GetValue(parameters) ?? DBNull.Value;
            command.Parameters.Add(parameter);
        }
    }

    public async Task WaitForDatabaseReadyAsync(string databaseType, TimeSpan? timeout = null)
    {
        timeout ??= TimeSpan.FromSeconds(30);
        var endTime = DateTime.UtcNow.Add(timeout.Value);

        while (DateTime.UtcNow < endTime)
        {
            try
            {
                switch (databaseType.ToLowerInvariant())
                {
                    case "mongodb":
                        var mongoDb = GetMongoDatabase();
                        await mongoDb.RunCommandAsync<object>("{ ping: 1 }");
                        break;
                    
                    case "postgresql":
                        using (var pgConnection = await GetPostgresConnectionAsync())
                        {
                            await pgConnection.CloseAsync();
                        }
                        break;
                    
                    case "mysql":
                        using (var mysqlConnection = await GetMySqlConnectionAsync())
                        {
                            await mysqlConnection.CloseAsync();
                        }
                        break;
                    
                    default:
                        throw new ArgumentException($"Unknown database type: {databaseType}");
                }

                _logger.LogInformation("{DatabaseType} database is ready", databaseType);
                return;
            }
            catch (Exception ex)
            {
                _logger.LogDebug("Waiting for {DatabaseType} database: {Error}", databaseType, ex.Message);
                await Task.Delay(TimeSpan.FromSeconds(1));
            }
        }

        throw new TimeoutException($"{databaseType} database was not ready within {timeout}");
    }

    #endregion
}