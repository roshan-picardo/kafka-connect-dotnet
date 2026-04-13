using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.MongoDb.Models;
using MongoDB.Driver;

namespace Kafka.Connect.MongoDb.Collections;

public class MongoClientProvider(Plugin.Providers.IConfigurationProvider configurationProvider)
    : IMongoClientProvider
{
    private readonly ConcurrentDictionary<string, IMongoClient> _clientCache = new();

    public IMongoClient GetMongoClient(string connector, int taskId)
    {
        var key = $"{connector}-{taskId:00}";
        return _clientCache.GetOrAdd(key, _ =>
        {
            var pluginConfig = configurationProvider.GetPluginConfig<PluginConfig>(connector);
            if (pluginConfig == null)
                throw new InvalidOperationException($"Unable to find the configuration matching {connector}.");
            var settings = MongoClientSettings.FromConnectionString(pluginConfig.ConnectionUri);
            settings.ApplicationName = key;
            return new MongoClient(settings);
        });
    }
}