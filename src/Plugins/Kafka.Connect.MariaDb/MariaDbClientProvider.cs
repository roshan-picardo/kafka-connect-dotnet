using System.Collections.Concurrent;
using Kafka.Connect.MariaDb.Models;
using MySqlConnector;

namespace Kafka.Connect.MariaDb;

public interface IMariaDbClientProvider
{
    IMariaDbClient GetMariaDbClient(string connector, int taskId = -1);
}

public class MariaDbClientProvider(Plugin.Providers.IConfigurationProvider configurationProvider)
    : IMariaDbClientProvider
{
    private readonly ConcurrentDictionary<string, IMariaDbClient> _clientCache = new();

    public IMariaDbClient GetMariaDbClient(string connector, int taskId = -1)
    {
        // If taskId is -1, use taskId 1 as default
        var actualTaskId = taskId == -1 ? 1 : taskId;
        var key = $"{connector}-{actualTaskId:00}";
        
        return _clientCache.GetOrAdd(key, _ =>
        {
            var pluginConfig = configurationProvider.GetPluginConfig<PluginConfig>(connector);
            if (pluginConfig == null)
                throw new InvalidOperationException($"Unable to find the configuration matching {connector}.");
            
            return new MariaDbClient(key, new MySqlConnection(pluginConfig.ConnectionString));
        });
    }
}
