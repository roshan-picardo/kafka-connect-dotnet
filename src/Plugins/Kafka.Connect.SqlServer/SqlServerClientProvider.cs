using System.Collections.Concurrent;
using Kafka.Connect.SqlServer.Models;
using Microsoft.Data.SqlClient;

namespace Kafka.Connect.SqlServer;

public interface ISqlServerClientProvider
{
    ISqlServerClient GetSqlServerClient(string connector, int taskId = -1);
}

public class SqlServerClientProvider(Plugin.Providers.IConfigurationProvider configurationProvider)
    : ISqlServerClientProvider
{
    private readonly ConcurrentDictionary<string, ISqlServerClient> _clientCache = new();

    public ISqlServerClient GetSqlServerClient(string connector, int taskId = -1)
    {
        var actualTaskId = taskId == -1 ? 1 : taskId;
        var key = $"{connector}-{actualTaskId:00}";
        
        return _clientCache.GetOrAdd(key, _ =>
        {
            var pluginConfig = configurationProvider.GetPluginConfig<PluginConfig>(connector);
            if (pluginConfig == null)
                throw new InvalidOperationException($"Unable to find the configuration matching {connector}.");
            
            return new SqlServerClient(key, new SqlConnection(pluginConfig.ConnectionString));
        });
    }
}
