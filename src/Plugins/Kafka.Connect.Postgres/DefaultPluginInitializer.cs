using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Postgres.Models;
using Kafka.Connect.Postgres.Strategies;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Microsoft.Extensions.Configuration;

namespace Kafka.Connect.Postgres;

public class DefaultPluginInitializer : PluginInitializer
{
    public override void AddServices(IServiceCollection collection, IConfiguration configuration, params (string Name, int Tasks)[] connectors)
    {
        collection
            .AddScoped<IPluginHandler, PostgresPluginHandler>()
            .AddScoped<IPluginInitializer, DefaultPluginInitializer>()
            .AddScoped<IStrategy, InsertStrategy>()
            .AddScoped<IStrategy, UpdateStrategy>()
            .AddScoped<IStrategy, UpsertStrategy>()
            .AddScoped<IStrategy, DeleteStrategy>()
            .AddScoped<IStrategy, ReadStrategy>()
            .AddScoped<IPostgresClientProvider, PostgresClientProvider>()
            .AddScoped<IPostgresCommandHandler, PostgresCommandHandler>();
        AddPostgresClients(collection, connectors);
    }
    
    private static void AddPostgresClients(IServiceCollection collection, (string Name, int Tasks)[] connectors)
    {
        foreach (var connector in connectors)
        {
            for (var t = 0; t < connector.Tasks; t++)
            {
                var taskId = t + 1;
                collection.AddSingleton<IPostgresClient>(provider =>
                {
                    var configurationProvider = provider.GetService<Plugin.Providers.IConfigurationProvider>() ??
                                                throw new InvalidOperationException(
                                                    $"Unable to resolve service for type 'IConfigurationProvider' for {connector.Name}.");
                    var postgresSinkConfig = configurationProvider.GetPluginConfig<PluginConfig>(connector.Name);
                    if (postgresSinkConfig == null)
                        throw new InvalidOperationException(
                            $"Unable to find the configuration matching {connector.Name}.");
                    return new PostgresClient($"{connector.Name}-{taskId:00}", new NpgsqlConnection(postgresSinkConfig.ConnectionString));
                });
            }
        }
    }
}