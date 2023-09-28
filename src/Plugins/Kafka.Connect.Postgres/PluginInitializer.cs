using Kafka.Connect.Plugin;
using Kafka.Connect.Postgres.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Kafka.Connect.Postgres;

public class PluginInitializer : IPluginInitializer
{
    public void AddServices(
        IServiceCollection collection,
        IConfiguration configuration,
        (string Plugin, IEnumerable<string> Connectors) pluginConfig)
    {
        collection.AddScoped<IPostgresClientProvider, PostgresClientProvider>();
        AddPostgresClients(collection, pluginConfig.Plugin, pluginConfig.Connectors);
    }

    private static void AddPostgresClients(IServiceCollection collection, string plugin, IEnumerable<string> connectors)
    {
        foreach (var connector in connectors)
        {
            collection.AddSingleton<IPostgresClient>(provider =>
            {
                var configurationProvider = provider.GetService<Plugin.Providers.IConfigurationProvider>() ?? throw new InvalidOperationException($"Unable to resolve service for type 'IConfigurationProvider' for {plugin} and {connector}.");
                var postgresSinkConfig = configurationProvider.GetSinkConfigProperties<PostgresSinkConfig>(connector, plugin);
                if (postgresSinkConfig == null) throw new InvalidOperationException($"Unable to find the configuration matching {plugin} and {connector}.");
                return new PostgresClient(connector, new NpgsqlConnection(postgresSinkConfig.ToString()));
            });
        }
    }
}