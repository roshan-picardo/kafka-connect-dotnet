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
        var connectionString = Configuration.GetServiceEndpoint("Db2");

        for (var attempt = 1; attempt <= 180; attempt++)
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
                if (attempt == 180)
                {
                    throw new TimeoutException(
                        $"Failed to start {GetTargetName()} after 180 attempts", ex);
                }

                LogMessage($"Starting: {GetTargetName()} (attempt: {attempt}/180)", "");
                await Task.Delay(ReadyDelayMs);
            }
        }
    }

    protected override async Task ExecuteScriptsAsync(string database, string[] scripts)
    {
        await using var connection = new DB2Connection(Configuration.GetServiceEndpoint("Db2"));
        await connection.OpenAsync();

        foreach (var script in scripts)
        {
            await using var command = new DB2Command(script, connection);
            await command.ExecuteNonQueryAsync();
        }
    }
}