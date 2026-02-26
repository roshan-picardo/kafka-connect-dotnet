using DotNet.Testcontainers.Networks;
using Npgsql;

namespace IntegrationTests.Kafka.Connect.Infrastructure.Fixtures;

public class PostgresFixture : DatabaseFixture
{
    public PostgresFixture(
        TestConfiguration configuration,
        Action<string, string> logMessage,
        IContainerService containerService,
        INetwork network,
        TestCaseConfig[]? testConfigs)
        : base(configuration, logMessage, containerService, network, testConfigs)
    {
    }

    protected override string GetTargetName() => "postgres";

    public override async Task WaitForReadyAsync()
    {
        var connectionString = Configuration.GetServiceEndpoint("Postgres");

        for (var attempt = 1; attempt <= DatabaseReadyMaxAttempts; attempt++)
        {
            try
            {
                await using var connection = new NpgsqlConnection(connectionString);
                await connection.OpenAsync();

                var command = new NpgsqlCommand("SELECT version()", connection);
                await command.ExecuteScalarAsync();

                LogMessage($"Postgres is ready (attempt {attempt})", "");
                return;
            }
            catch (Exception ex)
            {
                if (attempt == DatabaseReadyMaxAttempts)
                {
                    throw new TimeoutException(
                        $"Postgres did not become ready after {DatabaseReadyMaxAttempts} attempts", ex);
                }

                LogMessage($"Postgres not ready yet (attempt {attempt}/{DatabaseReadyMaxAttempts}): {ex.Message}",
                    "");
                await Task.Delay(DatabaseReadyDelayMs);
            }
        }
    }

    public override async Task ExecuteScriptsAsync(string database, string[] scripts)
    {
        var connectionString = Configuration.GetServiceEndpoint("Postgres");
        var builder = new NpgsqlConnectionStringBuilder(connectionString)
        {
            Database = database
        };

        await using var connection = new NpgsqlConnection(builder.ConnectionString);
        await connection.OpenAsync();

        foreach (var script in scripts)
        {
            await using var command = new NpgsqlCommand(script, connection);
            await command.ExecuteNonQueryAsync();
        }
    }
}
