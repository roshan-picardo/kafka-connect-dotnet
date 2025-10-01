using Xunit;

namespace Kafka.Connect.Tests;

[CollectionDefinition("IntegrationTests")]
public class IntegrationTestCollection : ICollectionFixture<IntegrationTestFixture>
{
    // This class has no code, and is never created. Its purpose is simply
    // to be the place to apply [CollectionDefinition] and all the
    // ICollectionFixture<> interfaces.
}

public class IntegrationTestFixture : IAsyncLifetime
{
    public async Task InitializeAsync()
    {
        // Global setup for all integration tests
        // This runs once before all tests in the collection
        await Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        // Global cleanup for all integration tests
        // This runs once after all tests in the collection
        await Task.CompletedTask;
    }
}