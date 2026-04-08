using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.MongoDb.Models;
using MongoDB.Driver;

namespace Kafka.Connect.MongoDb.Collections
{
    public class MongoClientProvider : IMongoClientProvider
    {
        private readonly Plugin.Providers.IConfigurationProvider _configurationProvider;
        private readonly ConcurrentDictionary<string, IMongoClient> _clientCache = new();

        public MongoClientProvider(Plugin.Providers.IConfigurationProvider configurationProvider)
        {
            _configurationProvider = configurationProvider;
        }

        public IMongoClient GetMongoClient(string connector, int taskId)
        {
            var key = $"{connector}-{taskId:00}";
            
            return _clientCache.GetOrAdd(key, _ =>
            {
                var pluginConfig = _configurationProvider.GetPluginConfig<PluginConfig>(connector);
                if (pluginConfig == null)
                    throw new InvalidOperationException($"Unable to find the configuration matching {connector}.");
                
                var settings = MongoClientSettings.FromConnectionString(pluginConfig.ConnectionUri);
                settings.ApplicationName = key;
                return new MongoClient(settings);
            });
        }
    }
}