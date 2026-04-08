using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Postgres.Models;
using Kafka.Connect.Postgres.Strategies;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Microsoft.Extensions.Configuration;

namespace Kafka.Connect.Postgres;

public class DefaultPluginInitializer : IPluginInitializer
{
    public void AddServices(IServiceCollection collection, IConfiguration configuration, params (string Name, int Tasks)[] connectors)
    {
        collection
            .AddScoped<IPluginHandler, PostgresPluginHandler>()
            .AddScoped<IPluginInitializer, DefaultPluginInitializer>()
            .AddScoped<IStrategy, InsertStrategy>()
            .AddScoped<IStrategy, UpdateStrategy>()
            .AddScoped<IStrategy, UpsertStrategy>()
            .AddScoped<IStrategy, DeleteStrategy>()
            .AddScoped<IStrategy, ReadStrategy>()
            .AddScoped<IStrategySelector, ChangelogStrategySelector>()
            .AddSingleton<IPostgresClientProvider, PostgresClientProvider>()
            .AddScoped<IPostgresCommandHandler, PostgresCommandHandler>();
    }
}