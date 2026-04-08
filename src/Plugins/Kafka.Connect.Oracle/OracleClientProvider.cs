using System.Collections.Concurrent;
using Kafka.Connect.Oracle.Models;
using Oracle.ManagedDataAccess.Client;

namespace Kafka.Connect.Oracle;

public interface IOracleClientProvider
{
    IOracleClient GetOracleClient(string connector, int taskId = -1);
}

public class OracleClientProvider(Plugin.Providers.IConfigurationProvider configurationProvider)
    : IOracleClientProvider
{
    private readonly ConcurrentDictionary<string, IOracleClient> _clientCache = new();

    public IOracleClient GetOracleClient(string connector, int taskId = -1)
    {
        // If taskId is -1, use taskId 1 as default
        var actualTaskId = taskId == -1 ? 1 : taskId;
        var key = $"{connector}-{actualTaskId:00}";
        
        return _clientCache.GetOrAdd(key, _ =>
        {
            var pluginConfig = configurationProvider.GetPluginConfig<PluginConfig>(connector);
            if (pluginConfig == null)
                throw new InvalidOperationException($"Unable to find the configuration matching {connector}.");
            
            return new OracleClient(key, new OracleConnection(pluginConfig.ConnectionString));
        });
    }
}
