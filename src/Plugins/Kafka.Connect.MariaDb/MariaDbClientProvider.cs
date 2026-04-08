using System;
using System.Collections.Concurrent;
using Kafka.Connect.MariaDb.Models;
using MySql.Data.MySqlClient;

namespace Kafka.Connect.MariaDb;

public interface IMariaDbClientProvider
{
    IMariaDbClient GetMariaDbClient(string connector, int taskId = -1);
}

public class MariaDbClientProvider : IMariaDbClientProvider
{
    private readonly Plugin.Providers.IConfigurationProvider _configurationProvider;
    private readonly ConcurrentDictionary<string, IMariaDbClient> _clientCache = new();

    public MariaDbClientProvider(Plugin.Providers.IConfigurationProvider configurationProvider)
    {
        _configurationProvider = configurationProvider;
    }

    public IMariaDbClient GetMariaDbClient(string connector, int taskId = -1)
    {
        // If taskId is -1, use taskId 1 as default
        var actualTaskId = taskId == -1 ? 1 : taskId;
        var key = $"{connector}-{actualTaskId:00}";
        
        return _clientCache.GetOrAdd(key, _ =>
        {
            var pluginConfig = _configurationProvider.GetPluginConfig<PluginConfig>(connector);
            if (pluginConfig == null)
                throw new InvalidOperationException($"Unable to find the configuration matching {connector}.");
            
            return new MariaDbClient(key, new MySqlConnection(pluginConfig.ConnectionString));
        });
    }
}
