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
            .AddScoped<IOracleClientProvider, OracleClientProvider>()
            .AddScoped<IOracleCommandHandler, OracleCommandHandler>();
        AddOracleClients(collection, connectors);
    }
    
    private static void AddOracleClients(IServiceCollection collection, (string Name, int Tasks)[] connectors)
    {
        foreach (var connector in connectors)
        {
            for (var t = 0; t < connector.Tasks; t++)
            {
                var taskId = t + 1;
                collection.AddSingleton<IOracleClient>(provider =>
                {
                    var configurationProvider = provider.GetService<Plugin.Providers.IConfigurationProvider>() ??
                                                throw new InvalidOperationException(
                                                    $"Unable to resolve service for type 'IConfigurationProvider' for {connector.Name}.");
                    var oracleConfig = configurationProvider.GetPluginConfig<PluginConfig>(connector.Name);
                    if (oracleConfig == null)
                        throw new InvalidOperationException(
                            $"Unable to find the configuration matching {connector.Name}.");
                    return new OracleClient($"{connector.Name}-{taskId:00}", new OracleConnection(oracleConfig.ConnectionString));
                });
            }
        }
    }
}
