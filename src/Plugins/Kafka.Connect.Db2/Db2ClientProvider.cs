using System.Collections.Concurrent;
using Kafka.Connect.Db2.Models;
using IBM.Data.Db2;

namespace Kafka.Connect.Db2;

public interface IDb2ClientProvider
{
    IDb2Client GetDb2Client(string connector, int taskId = -1);
}

public class Db2ClientProvider(Plugin.Providers.IConfigurationProvider configurationProvider)
    : IDb2ClientProvider
{
    private readonly ConcurrentDictionary<string, IDb2Client> _clientCache = new();

    public IDb2Client GetDb2Client(string connector, int taskId = -1)
    {
        var actualTaskId = taskId == -1 ? 1 : taskId;
        var key = $"{connector}-{actualTaskId:00}";
        
        return _clientCache.GetOrAdd(key, _ =>
        {
            var pluginConfig = configurationProvider.GetPluginConfig<PluginConfig>(connector);
            if (pluginConfig == null)
                throw new InvalidOperationException($"Unable to find the configuration matching {connector}.");
            
            return new Db2Client(key, new DB2Connection(pluginConfig.ConnectionString));
        });
    }
}
