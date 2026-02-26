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

        for (var attempt = 1; attempt <= DatabaseReadyMaxAttempts; attempt++)
        {
            try
            {
                var client = new MongoClient(connectionString);
                await client.ListDatabaseNamesAsync();

                LogMessage($"MongoDB is ready (attempt {attempt})", "");
                return;
            }
            catch (Exception ex)
            {
                if (attempt == DatabaseReadyMaxAttempts)
                {
                    throw new TimeoutException(
                        $"MongoDB did not become ready after {DatabaseReadyMaxAttempts} attempts", ex);
                }

                LogMessage($"MongoDB not ready yet (attempt {attempt}/{DatabaseReadyMaxAttempts}): {ex.Message}", "");
                await Task.Delay(DatabaseReadyDelayMs);
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
                // Parse the script as a MongoDB command JSON document
                var commandDoc = BsonDocument.Parse(script);
                await db.RunCommandAsync<BsonDocument>(commandDoc);
            }
            catch (Exception ex)
            {
                LogMessage($"Failed to execute MongoDB command: {script} - {ex.Message}", "");
                throw;
            }
        }
    }
}
