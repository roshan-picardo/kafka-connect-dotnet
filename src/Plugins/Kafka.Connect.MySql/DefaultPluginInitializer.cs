using Kafka.Connect.MySql.Models;
using Kafka.Connect.MySql.Strategies;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Strategies;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using MySql.Data.MySqlClient;

namespace Kafka.Connect.MySql;

public class DefaultPluginInitializer : IPluginInitializer
{
    public void AddServices(
        IServiceCollection collection,
        IConfiguration configuration,
        params (string Name, int Tasks)[] connectors)
    {
        collection
            .AddScoped<IPluginHandler, MySqlPluginHandler>()
            .AddScoped<IPluginInitializer, DefaultPluginInitializer>()
            .AddScoped<IStrategy, InsertStrategy>()
            .AddScoped<IStrategy, UpdateStrategy>()
            .AddScoped<IStrategy, UpsertStrategy>()
            .AddScoped<IStrategy, DeleteStrategy>()
            .AddScoped<IStrategy, ReadStrategy>()
            .AddScoped<IStrategySelector, ChangelogStrategySelector>()
            .AddSingleton<IMySqlClientProvider, MySqlClientProvider>()
            .AddScoped<IMySqlCommandHandler, MySqlCommandHandler>();
    }
}