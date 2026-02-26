using DotNet.Testcontainers.Networks;
using MySql.Data.MySqlClient;

namespace IntegrationTests.Kafka.Connect.Infrastructure.Fixtures;

public class MySqlFixture(
    TestConfiguration configuration,
    Action<string, string> logMessage,
    IContainerService containerService,
    INetwork network,
    TestCaseConfig[]? testConfigs)
    : DatabaseFixture(configuration, logMessage, containerService, network, testConfigs)
{
    protected override string GetTargetName() => "mysql";

    protected override async Task WaitForReadyAsync()
    {
        var connectionString = Configuration.GetServiceEndpoint("MySql");

        for (var attempt = 1; attempt <= DatabaseReadyMaxAttempts; attempt++)
        {
            try
            {
                await using var connection = new MySqlConnection(connectionString);
                await connection.OpenAsync();

                var command = new MySqlCommand("SELECT VERSION()", connection);
                await command.ExecuteScalarAsync();

                LogMessage($"MySQL is ready (attempt {attempt})", "");
                return;
            }
            catch (Exception ex)
            {
                if (attempt == DatabaseReadyMaxAttempts)
                {
                    throw new TimeoutException(
                        $"MySQL did not become ready after {DatabaseReadyMaxAttempts} attempts", ex);
                }

                LogMessage($"MySQL not ready yet (attempt {attempt}/{DatabaseReadyMaxAttempts}): {ex.Message}", "");
                await Task.Delay(DatabaseReadyDelayMs);
            }
        }
    }

    protected override async Task ExecuteScriptsAsync(string database, string[] scripts)
    {
        var connectionString = Configuration.GetServiceEndpoint("MySql");
        var builder = new MySqlConnectionStringBuilder(connectionString)
        {
            Database = database
        };

        await using var connection = new MySqlConnection(builder.ConnectionString);
        await connection.OpenAsync();

        foreach (var script in scripts)
        {
            await using var command = new MySqlCommand(script, connection);
            await command.ExecuteNonQueryAsync();
        }
    }
}
