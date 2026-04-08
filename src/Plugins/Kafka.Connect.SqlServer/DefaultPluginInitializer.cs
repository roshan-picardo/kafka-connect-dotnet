using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.SqlServer.Models;
using Kafka.Connect.SqlServer.Strategies;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Kafka.Connect.SqlServer;

public class DefaultPluginInitializer : IPluginInitializer
{
    public void AddServices(
        IServiceCollection collection,
        IConfiguration configuration,
        params (string Name, int Tasks)[] connectors)
    {
        collection
            .AddScoped<IPluginHandler, SqlServerPluginHandler>()
            .AddScoped<IPluginInitializer, DefaultPluginInitializer>()
            .AddScoped<IStrategy, InsertStrategy>()
            .AddScoped<IStrategy, UpdateStrategy>()
            .AddScoped<IStrategy, UpsertStrategy>()
            .AddScoped<IStrategy, DeleteStrategy>()
            .AddScoped<IStrategy, ReadStrategy>()
            .AddScoped<IStrategySelector, ChangelogStrategySelector>()
            .AddSingleton<ISqlServerClientProvider, SqlServerClientProvider>()
            .AddScoped<ISqlServerCommandHandler, SqlServerCommandHandler>();
    }
}
