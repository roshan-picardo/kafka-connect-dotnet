using System.Collections.Concurrent;
using Cassandra;
using Kafka.Connect.Cassandra.Models;

namespace Kafka.Connect.Cassandra;

public interface ICassandraClient
{
    string ApplicationName { get; }
    ISession GetSession();
}

public class CassandraClient(string connector, ISession session) : ICassandraClient
{
    public string ApplicationName { get; } = connector;
    public ISession GetSession() => session;
}

public interface ICassandraClientProvider
{
    ICassandraClient GetCassandraClient(string connector, int taskId = -1);
}

public class CassandraClientProvider(Plugin.Providers.IConfigurationProvider configurationProvider)
    : ICassandraClientProvider
{
    private readonly ConcurrentDictionary<string, ICassandraClient> _clientCache = new();

    public ICassandraClient GetCassandraClient(string connector, int taskId = -1)
    {
        var actualTaskId = taskId == -1 ? 1 : taskId;
        var key = $"{connector}-{actualTaskId:00}";

        return _clientCache.GetOrAdd(key, _ =>
        {
            var pluginConfig = configurationProvider.GetPluginConfig<PluginConfig>(connector);
            if (pluginConfig == null)
            {
                throw new InvalidOperationException($"Unable to find the configuration matching {connector}.");
            }

            if (pluginConfig.Hosts == null || pluginConfig.Hosts.Length == 0)
            {
                throw new InvalidOperationException($"No Cassandra hosts were configured for connector {connector}.");
            }

            var clusterBuilder = Cluster.Builder().WithPort(pluginConfig.Port);
            clusterBuilder.AddContactPoints(pluginConfig.Hosts);

            var cluster = clusterBuilder.Build();
            var session = string.IsNullOrWhiteSpace(pluginConfig.Keyspace)
                ? cluster.Connect()
                : cluster.Connect(pluginConfig.Keyspace);

            return new CassandraClient(key, session);
        });
    }
}
