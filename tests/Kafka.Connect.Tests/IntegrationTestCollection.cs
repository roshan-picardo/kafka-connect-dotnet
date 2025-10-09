using Xunit;

namespace Kafka.Connect.Tests;

/// <summary>
/// Collection definition for integration tests to ensure they run sequentially
/// and share the same test environment setup/teardown.
/// </summary>
[CollectionDefinition("IntegrationTestCollection")]
public class IntegrationTestCollection : ICollectionFixture<IntegrationTestFixture>
{
    // This class has no code, and is never created. Its purpose is simply
    // to be the place to apply [CollectionDefinition] and all the
    // ICollectionFixture<> interfaces.
}

/// <summary>
/// Shared fixture for integration tests that sets up common resources
/// and ensures proper cleanup.
/// </summary>
public class IntegrationTestFixture : IAsyncLifetime
{
    public async Task InitializeAsync()
    {
        // Global setup for all integration tests
        // This runs once before any test in the collection
        await Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        // Global cleanup for all integration tests
        // This runs once after all tests in the collection complete
        await Task.CompletedTask;
    }
}