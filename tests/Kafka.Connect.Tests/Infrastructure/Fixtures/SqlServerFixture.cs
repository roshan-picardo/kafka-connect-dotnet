using DotNet.Testcontainers.Networks;
using Microsoft.Data.SqlClient;
using System.Text.Json;

namespace IntegrationTests.Kafka.Connect.Infrastructure.Fixtures;

public class SqlServerFixture : DatabaseFixture
{
    public SqlServerFixture(
        TestConfiguration configuration,
        Action<string, string> logMessage,
        IContainerService containerService,
        INetwork network,
        TestCaseConfig[]? testConfigs)
        : base(configuration, logMessage, containerService, network, testConfigs)
    {
    }

    protected override string GetTargetName() => "sqlserver";

    public override async Task InitializeAsync()
    {
        var targetName = GetTargetName();
        LogMessage($"Initializing {targetName} infrastructure...", "");
        
        // Create database container
        await CreateContainersAsync();
        
        // Wait for database to be ready
        await WaitForReadyAsync();
        
        // Create databases from connector configurations BEFORE executing scripts
        await CreateDatabasesFromConfigurationsAsync();
        
        // Execute setup scripts if test configurations are available
        if (TestConfigs != null)
        {
            var testConfig = TestConfigs.FirstOrDefault(tc =>
                tc.Target?.Equals(targetName, StringComparison.OrdinalIgnoreCase) == true);
            
            if (testConfig?.Setup?.Scripts != null && testConfig.Setup.Scripts.Length > 0)
            {
                LogMessage($"Executing setup scripts for {targetName}...", "");
                await ExecuteScriptsAsync(testConfig.Setup.Database ?? string.Empty, testConfig.Setup.Scripts);
                LogMessage($"Setup scripts executed for {targetName}!", "");
            }
        }
        
        LogMessage($"{targetName} infrastructure initialized!", "");
    }

    public override async Task WaitForReadyAsync()
    {
        var connectionString = Configuration.GetServiceEndpoint("SqlServer");
        var builder = new SqlConnectionStringBuilder(connectionString)
        {
            InitialCatalog = "master",
            ConnectTimeout = 5
        };

        for (var attempt = 1; attempt <= DatabaseReadyMaxAttempts; attempt++)
        {
            try
            {
                await using var connection = new SqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                var command = new SqlCommand("SELECT @@VERSION", connection);
                await command.ExecuteScalarAsync();

                LogMessage($"SQL Server is ready (attempt {attempt})", "");
                return;
            }
            catch (Exception ex)
            {
                if (attempt == DatabaseReadyMaxAttempts)
                {
                    throw new TimeoutException(
                        $"SQL Server did not become ready after {DatabaseReadyMaxAttempts} attempts", ex);
                }

                LogMessage($"SQL Server not ready yet (attempt {attempt}/{DatabaseReadyMaxAttempts}): {ex.Message}",
                    "");
                await Task.Delay(DatabaseReadyDelayMs);
            }
        }
    }

    public override async Task ExecuteScriptsAsync(string database, string[] scripts)
    {
        var connectionString = Configuration.GetServiceEndpoint("SqlServer");
        var builder = new SqlConnectionStringBuilder(connectionString)
        {
            InitialCatalog = database
        };

        await using var connection = new SqlConnection(builder.ConnectionString);
        await connection.OpenAsync();

        foreach (var script in scripts)
        {
            await using var command = new SqlCommand(script, connection);
            await command.ExecuteNonQueryAsync();
        }
    }

    public async Task CreateDatabasesFromConfigurationsAsync()
    {
        LogMessage("Creating SQL Server databases from connector configurations...", "");

        var configFiles = Directory.GetFiles(Path.Join(Directory.GetCurrentDirectory(), "Configurations"),
            "appsettings.*.json");

        if (configFiles.Length == 0)
        {
            LogMessage("No connector configuration files found, skipping SQL Server database creation", "");
            return;
        }

        var sqlServerDatabases = new HashSet<string>();

        foreach (var configFile in configFiles)
        {
            try
            {
                var configContent = await File.ReadAllTextAsync(configFile);
                var configJson = JsonDocument.Parse(configContent);

                if (configJson.RootElement.TryGetProperty("worker", out var worker) &&
                    worker.TryGetProperty("connectors", out var connectors))
                {
                    foreach (var connector in connectors.EnumerateObject())
                    {
                        if (connector.Value.TryGetProperty("plugin", out var plugin) &&
                            plugin.TryGetProperty("name", out var pluginName) &&
                            pluginName.GetString() == "sqlserver" &&
                            plugin.TryGetProperty("properties", out var properties) &&
                            properties.TryGetProperty("database", out var database))
                        {
                            var databaseName = database.GetString();
                            if (!string.IsNullOrEmpty(databaseName) && databaseName != "master")
                            {
                                sqlServerDatabases.Add(databaseName);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogMessage($"Failed to parse config file {Path.GetFileName(configFile)}: {ex.Message}", "");
            }
        }

        foreach (var databaseName in sqlServerDatabases)
        {
            try
            {
                await CreateDatabaseAsync(databaseName);
                LogMessage($"Created SQL Server database: {databaseName}", "");
            }
            catch (Exception ex)
            {
                LogMessage($"Failed to create SQL Server database {databaseName}: {ex.Message}", "");
            }
        }

        LogMessage($"SQL Server database creation completed. Created {sqlServerDatabases.Count} databases.", "");
    }

    private async Task CreateDatabaseAsync(string databaseName)
    {
        var connectionString = Configuration.GetServiceEndpoint("SqlServer");

        // Connect to master database to create the target database
        var builder = new SqlConnectionStringBuilder(connectionString)
        {
            InitialCatalog = "master"
        };

        await using var connection = new SqlConnection(builder.ConnectionString);
        await connection.OpenAsync();

        // Check if database exists
        var checkCommand = new SqlCommand("SELECT COUNT(*) FROM sys.databases WHERE name = @dbName", connection);
        checkCommand.Parameters.AddWithValue("@dbName", databaseName);
        var exists = (int)await checkCommand.ExecuteScalarAsync()! > 0;

        if (!exists)
        {
            var createCommand = new SqlCommand($"CREATE DATABASE [{databaseName}]", connection);
            await createCommand.ExecuteNonQueryAsync();
        }
    }
}
