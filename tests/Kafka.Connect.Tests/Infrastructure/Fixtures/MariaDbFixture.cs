using DotNet.Testcontainers.Networks;
using MySqlConnector;

namespace IntegrationTests.Kafka.Connect.Infrastructure.Fixtures;

public class MariaDbFixture(
    TestConfiguration configuration,
    Action<string, string> logMessage,
    IContainerService containerService,
    INetwork network,
    TestCaseConfig[]? testConfigs)
    : DatabaseFixture(configuration, logMessage, containerService, network, testConfigs)
{
    protected override string GetTargetName() => "mariadb";

    protected override async Task WaitForReadyAsync()
    {
        var connectionString = Configuration.GetServiceEndpoint("MariaDb");

        for (var attempt = 1; attempt <= ReadyMaxAttempts; attempt++)
        {
            try
            {
                await using var connection = new MySqlConnection(connectionString);
                await connection.OpenAsync();

                var command = new MySqlCommand("SELECT VERSION()", connection);
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
        var connectionString = Configuration.GetServiceEndpoint("MariaDb");
        var builder = new MySqlConnectionStringBuilder(connectionString ?? string.Empty)
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
