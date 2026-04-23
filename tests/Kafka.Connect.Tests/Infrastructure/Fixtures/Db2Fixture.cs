using DotNet.Testcontainers.Networks;
using IBM.Data.Db2;

namespace IntegrationTests.Kafka.Connect.Infrastructure.Fixtures;

public class Db2Fixture(
    TestConfiguration configuration,
    Action<string, string> logMessage,
    IContainerService containerService,
    INetwork network,
    TestCaseConfig[]? testConfigs)
    : DatabaseFixture(configuration, logMessage, containerService, network, testConfigs)
{
    protected override string GetTargetName() => "db2";

    protected override async Task WaitForReadyAsync()
    {
        var connectionString = BuildDb2ConnectionString();

        for (var attempt = 1; attempt <= ReadyMaxAttempts; attempt++)
        {
            try
            {
                await using var connection = new DB2Connection(connectionString);
                await connection.OpenAsync();

                await using var command = new DB2Command("SELECT 1 FROM SYSIBM.SYSDUMMY1", connection);
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

                var transientMessage = IsDb2StartupTransient(ex)
                    ? "DB2 is not accepting connections yet (SQL1035/57019), waiting for startup to complete"
                    : ex.Message;

                LogMessage($"Starting: {GetTargetName()} (attempt: {attempt}/{ReadyMaxAttempts})", transientMessage);
                await Task.Delay(ReadyDelayMs);
            }
        }
    }

    protected override async Task ExecuteScriptsAsync(string database, string[] scripts)
    {
        await using var connection = new DB2Connection(BuildDb2ConnectionString());
        await connection.OpenAsync();

        foreach (var script in scripts)
        {
            await using var command = new DB2Command(script, connection);
            await command.ExecuteNonQueryAsync();
        }
    }

    private string BuildDb2ConnectionString()
    {
        var configured = Configuration.GetServiceEndpoint("Db2") ?? string.Empty;

        // Ensure each attempt fails fast while DB2 is still bootstrapping.
        if (!configured.Contains("Connect Timeout=", StringComparison.OrdinalIgnoreCase) &&
            !configured.Contains("Connection Timeout=", StringComparison.OrdinalIgnoreCase))
        {
            configured = $"{configured}Connect Timeout=5;";
        }

        return configured;
    }

    private static bool IsDb2StartupTransient(Exception ex)
    {
        var message = ex.Message;
        return message.Contains("SQL1035N", StringComparison.OrdinalIgnoreCase)
            || message.Contains("SQLSTATE=57019", StringComparison.OrdinalIgnoreCase);
    }
}