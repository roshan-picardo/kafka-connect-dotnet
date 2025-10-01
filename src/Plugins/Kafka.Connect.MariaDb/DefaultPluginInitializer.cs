using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.MariaDb.Models;
using Kafka.Connect.MariaDb.Strategies;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
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
            .AddScoped<IMariaDbClientProvider, MariaDbClientProvider>()
            .AddScoped<IMariaDbCommandHandler, MariaDbCommandHandler>();
        AddMariaDbClients(collection, connectors);
    }
    
    private static void AddMariaDbClients(IServiceCollection collection, (string Name, int Tasks)[] connectors)
    {
        foreach (var connector in connectors)
        {
            for (var t = 0; t < connector.Tasks; t++)
            {
                var taskId = t + 1;
                collection.AddSingleton<IMariaDbClient>(provider =>
                {
                    var configurationProvider = provider.GetService<Plugin.Providers.IConfigurationProvider>() ??
                                                throw new InvalidOperationException(
                                                    $"Unable to resolve service for type 'IConfigurationProvider' for {connector.Name}.");
                    var mariaDbConfig = configurationProvider.GetPluginConfig<PluginConfig>(connector.Name);
                    if (mariaDbConfig == null)
                        throw new InvalidOperationException(
                            $"Unable to find the configuration matching {connector.Name}.");
                    return new MariaDbClient($"{connector.Name}-{taskId:00}", new MySqlConnection(mariaDbConfig.ConnectionString));
                });
            }
        }
    }
}
