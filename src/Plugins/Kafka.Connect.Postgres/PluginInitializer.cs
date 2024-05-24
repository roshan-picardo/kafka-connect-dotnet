using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Strategies;
using Kafka.Connect.Postgres.Models;
using Kafka.Connect.Postgres.Strategies;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Kafka.Connect.Postgres;

public abstract class PluginInitializer : IPluginInitializer
{
    public void AddServices(
        IServiceCollection collection,
        IConfiguration configuration,
        (string Plugin, IEnumerable<(string Name, int Tasks)> Connectors) pluginConfig)
    {
        collection
            .AddScoped<ISinkHandler, PostgresSinkHandler>()
            .AddScoped<IPluginInitializer, DefaultPluginInitializer>()
            .AddScoped<IQueryStrategy, InsertStrategy>()
            .AddScoped<IQueryStrategy, UpdateStrategy>()
            .AddScoped<IQueryStrategy, UpsertStrategy>()
            .AddScoped<IQueryStrategy, DeleteStrategy>()
            .AddScoped<IQueryStrategy, ReadStrategy>()
            .AddScoped<IPostgresClientProvider, PostgresClientProvider>();
        AddPostgresClients(collection, pluginConfig.Plugin, pluginConfig.Connectors);
        AddAdditionalServices(collection, configuration);
    }

    private static void AddPostgresClients(IServiceCollection collection, string plugin, IEnumerable<(string Name, int Tasks)> connectors)
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
                                                    $"Unable to resolve service for type 'IConfigurationProvider' for {plugin} and {connector}.");
                    var postgresSinkConfig =
                        configurationProvider.GetSinkConfigProperties<PostgresSinkConfig>(connector.Name, plugin);
                    if (postgresSinkConfig == null)
                        throw new InvalidOperationException(
                            $"Unable to find the configuration matching {plugin} and {connector}.");
                    return new PostgresClient($"{connector.Name}-{taskId:00}", new NpgsqlConnection(postgresSinkConfig.ConnectionString));
                });
            }
        }
    }
    protected abstract void AddAdditionalServices(IServiceCollection collection, IConfiguration configuration);
}