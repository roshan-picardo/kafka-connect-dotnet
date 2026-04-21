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

        for (var attempt = 1; attempt <= ReadyMaxAttempts; attempt++)
        {
            try
            {
                await using var connection = new OracleConnection(connectionString);
                await connection.OpenAsync();

                var command = new OracleCommand("SELECT * FROM DUAL", connection);
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
