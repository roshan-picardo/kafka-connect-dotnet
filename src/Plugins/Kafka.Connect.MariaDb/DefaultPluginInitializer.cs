using Kafka.Connect.MariaDb.Models;
using Kafka.Connect.MariaDb.Strategies;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Strategies;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using MySqlConnector;

namespace Kafka.Connect.MariaDb;

public class DefaultPluginInitializer : IPluginInitializer
{
    public void AddServices(
        IServiceCollection collection,
        IConfiguration configuration,
        params (string Name, int Tasks)[] connectors)
    {
        collection
            .AddScoped<IPluginHandler, MariaDbPluginHandler>()
            .AddScoped<IPluginInitializer, DefaultPluginInitializer>()
            .AddScoped<IStrategy, InsertStrategy>()
            .AddScoped<IStrategy, UpdateStrategy>()
            .AddScoped<IStrategy, UpsertStrategy>()
            .AddScoped<IStrategy, DeleteStrategy>()
            .AddScoped<IStrategy, ReadStrategy>()
            .AddScoped<IStrategySelector, ChangelogStrategySelector>()
            .AddSingleton<IMariaDbClientProvider, MariaDbClientProvider>()
            .AddScoped<IMariaDbCommandHandler, MariaDbCommandHandler>();
    }
}
