using System;
using System.Collections.Concurrent;
using Kafka.Connect.MySql.Models;
using MySql.Data.MySqlClient;

namespace Kafka.Connect.MySql;

public interface IMySqlClientProvider
{
    IMySqlClient GetMySqlClient(string connector, int taskId = -1);
}

public class MySqlClientProvider : IMySqlClientProvider
{
    private readonly Plugin.Providers.IConfigurationProvider _configurationProvider;
    private readonly ConcurrentDictionary<string, IMySqlClient> _clientCache = new();

    public MySqlClientProvider(Plugin.Providers.IConfigurationProvider configurationProvider)
    {
        _configurationProvider = configurationProvider;
    }

    public IMySqlClient GetMySqlClient(string connector, int taskId = -1)
    {
        // If taskId is -1, use taskId 1 as default
        var actualTaskId = taskId == -1 ? 1 : taskId;
        var key = $"{connector}-{actualTaskId:00}";
        
        return _clientCache.GetOrAdd(key, _ =>
        {
            var pluginConfig = _configurationProvider.GetPluginConfig<PluginConfig>(connector);
            if (pluginConfig == null)
                throw new InvalidOperationException($"Unable to find the configuration matching {connector}.");
            
            return new MySqlClient(key, new MySqlConnection(pluginConfig.ConnectionString));
        });
    }
}
