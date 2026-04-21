using DotNet.Testcontainers.Networks;
using Microsoft.Data.SqlClient;
using System.Text.Json;
using Amazon.DynamoDBv2.Model;

namespace IntegrationTests.Kafka.Connect.Infrastructure.Fixtures;

public class SqlServerFixture(
    TestConfiguration configuration,
    Action<string, string> logMessage,
    IContainerService containerService,
    INetwork network,
    TestCaseConfig[]? testConfigs)
    : DatabaseFixture(configuration, logMessage, containerService, network, testConfigs)
{
    protected override string GetTargetName() => "sqlserver";

    public override async Task InitializeAsync()
    {
        var targetName = GetTargetName();
        
        await CreateContainersAsync();
        
        await WaitForReadyAsync();
        
        await CreateDatabasesFromConfigurationsAsync();

        var testConfig = TestConfigs?.FirstOrDefault(tc =>
            tc.Target?.Equals(targetName, StringComparison.OrdinalIgnoreCase) == true);
            
        if (testConfig?.Setup?.Scripts != null && testConfig.Setup.Scripts.Length > 0)
        {
            await ExecuteScriptsAsync(testConfig.Setup.Database ?? string.Empty, testConfig.Setup.Scripts);
        }
        LogMessage($"Infrastructure is ready: {GetTargetName()}", "");
    }

    protected override async Task WaitForReadyAsync()
    {
        var connectionString = Configuration.GetServiceEndpoint("SqlServer");
        var builder = new SqlConnectionStringBuilder(connectionString)
        {
            InitialCatalog = "master",
            ConnectTimeout = 5
        };

        for (var attempt = 1; attempt <= ReadyMaxAttempts; attempt++)
        {
            try
            {
                await using var connection = new SqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                var command = new SqlCommand("SELECT @@VERSION", connection);
                await command.ExecuteScalarAsync();

                LogMessage($"Started: {GetTargetName()}", "");
                return;
            }
            catch (Exception ex)
            {
                if (attempt == ReadyMaxAttempts)
                {
                    throw new TimeoutException(
                        $"Failed to start {GetTargetName()} after {ReadyMaxAttempts} attempts", ex);
                }

                LogMessage($"Starting: {GetTargetName()} (attempt: {attempt}/{ReadyMaxAttempts})", "");
                await Task.Delay(ReadyDelayMs);
            }
        }
    }

    protected override async Task ExecuteScriptsAsync(string database, string[] scripts)
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

    private async Task CreateDatabasesFromConfigurationsAsync()
    {
        var configDirectory = Path.Join(Directory.GetCurrentDirectory(), "Configurations");
        var configFiles = new List<string>();
        
        configFiles.AddRange(Directory.GetFiles(configDirectory, "appsettings.*.json"));
        
        // Also search in standalone subdirectory
        var standaloneDirectory = Path.Join(configDirectory, "standalone");
        if (Directory.Exists(standaloneDirectory))
        {
            configFiles.AddRange(Directory.GetFiles(standaloneDirectory, "appsettings.*.json"));
        }

        if (configFiles.Count == 0)
        {
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
                LogMessage($"Created {GetTargetName()} database: {databaseName}", "");
            }
            catch (Exception ex)
            {
                LogMessage($"Failed to create {GetTargetName()} database {databaseName}: {ex.Message}", "");
            }
        }

        LogMessage($"Completed {GetTargetName()} database creation. Created {sqlServerDatabases.Count} databases.", "");
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
        var result = await checkCommand.ExecuteScalarAsync();
        var exists = result != null && (int)result > 0;

        if (!exists)
        {
            var createCommand = new SqlCommand($"CREATE DATABASE [{databaseName}]", connection);
            await createCommand.ExecuteNonQueryAsync();
        }
    }
}
