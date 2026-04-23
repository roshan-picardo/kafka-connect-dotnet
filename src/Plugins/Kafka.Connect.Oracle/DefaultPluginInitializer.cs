using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Oracle.Models;
using Kafka.Connect.Oracle.Strategies;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Oracle.ManagedDataAccess.Client;

namespace Kafka.Connect.Oracle;

public class DefaultPluginInitializer : IPluginInitializer
{
    public void AddServices(
        IServiceCollection collection,
        IConfiguration configuration,
        params (string Name, int Tasks)[] connectors)
    {
        collection
            .AddScoped<IPluginHandler, OraclePluginHandler>()
            .AddScoped<IPluginInitializer, DefaultPluginInitializer>()
            .AddScoped<IStrategy, InsertStrategy>()
            .AddScoped<IStrategy, UpdateStrategy>()
            .AddScoped<IStrategy, UpsertStrategy>()
            .AddScoped<IStrategy, DeleteStrategy>()
            .AddScoped<IStrategy, ReadStrategy>()
            .AddScoped<IStrategySelector, ChangelogStrategySelector>()
            .AddSingleton<IOracleClientProvider, OracleClientProvider>()
            .AddScoped<IOracleCommandHandler, OracleCommandHandler>()
            .AddScoped<IOracleSqlExecutor, OracleSqlExecutor>();
    }
}
