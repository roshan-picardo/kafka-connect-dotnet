using System;
using System.Collections.Concurrent;
using Kafka.Connect.Postgres.Models;
using Npgsql;

namespace Kafka.Connect.Postgres;

public interface IPostgresClientProvider
{
    IPostgresClient GetPostgresClient(string connector, int taskId = -1);
}

public class PostgresClientProvider : IPostgresClientProvider
{
    private readonly Plugin.Providers.IConfigurationProvider _configurationProvider;
    private readonly ConcurrentDictionary<string, IPostgresClient> _clientCache = new();

    public PostgresClientProvider(Plugin.Providers.IConfigurationProvider configurationProvider)
    {
        _configurationProvider = configurationProvider;
    }

    public IPostgresClient GetPostgresClient(string connector, int taskId = -1)
    {
        // If taskId is -1, use taskId 1 as default
        var actualTaskId = taskId == -1 ? 1 : taskId;
        var key = $"{connector}-{actualTaskId:00}";
        
        return _clientCache.GetOrAdd(key, _ =>
        {
            var pluginConfig = _configurationProvider.GetPluginConfig<PluginConfig>(connector);
            if (pluginConfig == null)
                throw new InvalidOperationException($"Unable to find the configuration matching {connector}.");
            
            return new PostgresClient(key, new NpgsqlConnection(pluginConfig.ConnectionString));
        });
    }
}