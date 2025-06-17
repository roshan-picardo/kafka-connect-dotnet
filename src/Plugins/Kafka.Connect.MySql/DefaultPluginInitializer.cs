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
            .AddScoped<IMySqlClientProvider, MySqlClientProvider>()
            .AddScoped<IMySqlCommandHandler, MySqlCommandHandler>();
        AddMySqlClients(collection, connectors);
    }
    
    private static void AddMySqlClients(IServiceCollection collection, (string Name, int Tasks)[] connectors)
    {
        foreach (var connector in connectors)
        {
            for (var t = 0; t < connector.Tasks; t++)
            {
                var taskId = t + 1;
                collection.AddSingleton<IMySqlClient>(provider =>
                {
                    var configurationProvider = provider.GetService<Plugin.Providers.IConfigurationProvider>() ??
                                                throw new InvalidOperationException(
                                                    $"Unable to resolve service for type 'IConfigurationProvider' for {connector.Name}.");
                    var mySqlConfig = configurationProvider.GetPluginConfig<PluginConfig>(connector.Name);
                    if (mySqlConfig == null)
                        throw new InvalidOperationException(
                            $"Unable to find the configuration matching {connector.Name}.");
                    return new MySqlClient($"{connector.Name}-{taskId:00}", new MySqlConnection(mySqlConfig.ConnectionString));
                });
            }
        }
    }
}