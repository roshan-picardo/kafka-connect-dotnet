using DotNet.Testcontainers.Networks;
using Oracle.ManagedDataAccess.Client;

namespace IntegrationTests.Kafka.Connect.Infrastructure.Fixtures;

public class OracleFixture : DatabaseFixture
{
    public OracleFixture(
        TestConfiguration configuration,
        Action<string, string> logMessage,
        IContainerService containerService,
        INetwork network,
        TestCaseConfig[]? testConfigs)
        : base(configuration, logMessage, containerService, network, testConfigs)
    {
    }

    protected override string GetTargetName() => "oracle";

    public override async Task WaitForReadyAsync()
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

                LogMessage($"Oracle is ready (attempt {attempt})", "");
                return;
            }
            catch (Exception ex)
            {
                if (attempt == DatabaseReadyMaxAttempts)
                {
                    throw new TimeoutException(
                        $"Oracle did not become ready after {DatabaseReadyMaxAttempts} attempts", ex);
                }

                LogMessage($"Oracle not ready yet (attempt {attempt}/{DatabaseReadyMaxAttempts}): {ex.Message}", "");
                await Task.Delay(DatabaseReadyDelayMs);
            }
        }
    }

    public override async Task ExecuteScriptsAsync(string database, string[] scripts)
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
