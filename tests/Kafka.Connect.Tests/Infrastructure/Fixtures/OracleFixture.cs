using DotNet.Testcontainers.Networks;
using Oracle.ManagedDataAccess.Client;

namespace IntegrationTests.Kafka.Connect.Infrastructure.Fixtures;

public class OracleFixture(
    TestConfiguration configuration,
    Action<string, string> logMessage,
    IContainerService containerService,
    INetwork network,
    TestCaseConfig[]? testConfigs)
    : DatabaseFixture(configuration, logMessage, containerService, network, testConfigs)
{
    protected override string GetTargetName() => "oracle";

    protected override async Task WaitForReadyAsync()
    {
        var connectionString = Configuration.GetServiceEndpoint("Oracle");

        for (var attempt = 1; attempt <= DatabaseReadyMaxAttempts; attempt++)
        {
            try
            {
                await using var connection = new OracleConnection(connectionString);
                await connection.OpenAsync();

                var command = new OracleCommand("SELECT * FROM DUAL", connection);
                await command.ExecuteScalarAsync();

                LogMessage($"Service is ready: oracle", "");
                return;
            }
            catch (Exception ex)
            {
                if (attempt == DatabaseReadyMaxAttempts)
                {
                    throw new TimeoutException(
                        $"Service failed to start after {DatabaseReadyMaxAttempts} attempts: oracle", ex);
                }

                LogMessage($"Service failed to start: oracle ({attempt}/{DatabaseReadyMaxAttempts})", "");
                await Task.Delay(DatabaseReadyDelayMs);
            }
        }
    }

    protected override async Task ExecuteScriptsAsync(string database, string[] scripts)
    {
        var connectionString = Configuration.GetServiceEndpoint("Oracle");

        await using var connection = new OracleConnection(connectionString);
        await connection.OpenAsync();

        foreach (var script in scripts)
        {
            await using var command = new OracleCommand(script, connection);
            await command.ExecuteNonQueryAsync();
        }
    }
}
