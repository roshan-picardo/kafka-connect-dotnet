using DotNet.Testcontainers.Networks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace IntegrationTests.Kafka.Connect.Infrastructure.Fixtures;

public class MongoDbFixture(
    TestConfiguration configuration,
    Action<string, string> logMessage,
    IContainerService containerService,
    INetwork network,
    TestCaseConfig[]? testConfigs)
    : DatabaseFixture(configuration, logMessage, containerService, network, testConfigs)
{
    protected override string GetTargetName() => "mongodb";

    protected override async Task WaitForReadyAsync()
    {
        var connectionString = Configuration.GetServiceEndpoint("Mongo");

        for (var attempt = 1; attempt <= ReadyMaxAttempts; attempt++)
        {
            try
            {
                var client = new MongoClient(connectionString);
                await client.ListDatabaseNamesAsync();

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
        var connectionString = Configuration.GetServiceEndpoint("Mongo");
        var client = new MongoClient(connectionString);
        var db = client.GetDatabase(database);
        
        foreach (var script in scripts)
        {
            try
            {
                var commandDoc = BsonDocument.Parse(script);
                await db.RunCommandAsync<BsonDocument>(commandDoc);
            }
            catch (Exception ex)
            {
                LogMessage($"Failed to execute {GetTargetName()} command: {script} - {ex.Message}", "");
                throw;
            }
        }
    }
}
