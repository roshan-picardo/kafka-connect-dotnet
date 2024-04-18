using System;
using System.Collections.Generic;
using Kafka.Connect.MongoDb.Collections;
using Kafka.Connect.MongoDb.Models;
using Kafka.Connect.MongoDb.Strategies;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Strategies;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using MongoDB.Driver;

namespace Kafka.Connect.MongoDb
{
    public abstract class PluginInitializer : IPluginInitializer
    {
        public void AddServices(IServiceCollection collection, IConfiguration configuration, (string Plugin, IEnumerable<(string Name, int Tasks)> Connectors) pluginConfig)
        {
            try
            {
                collection
                    .AddScoped<ISinkHandler, MongoSinkHandler>()
                    .AddScoped<IPluginInitializer, DefaultPluginInitializer>()
                    .AddScoped<IMongoClientProvider, MongoClientProvider>()
                    .AddScoped<IReadWriteStrategy, ReadStrategy>()
                    .AddScoped<IReadWriteStrategy, DeleteStrategy>()
                    .AddScoped<IReadWriteStrategy, InsertStrategy>()
                    .AddScoped<IReadWriteStrategy, UpdateStrategy>()
                    .AddScoped<IReadWriteStrategy, UpsertStrategy>()
                    .AddScoped<IMongoQueryRunner, MongoQueryRunner>()
                    .AddScoped<ISourceHandler, MongoSourceHandler>();
                AddMongoClients(collection, pluginConfig.Plugin, pluginConfig.Connectors);
                AddAdditionalServices(collection, configuration);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
        
        private static void AddMongoClients(IServiceCollection collection, string plugin, IEnumerable<(string Name, int Tasks)> connectors)
        {
            foreach (var connector in connectors)
            {
                for (var t = 0; t < connector.Tasks; t++)
                {
                    var taskId = t + 1;
                    collection.AddSingleton<IMongoClient>(provider =>
                    {
                        var configurationProvider = provider.GetService<Plugin.Providers.IConfigurationProvider>() ??
                                                    throw new InvalidOperationException(
                                                        $@"Unable to resolve service for type 'IConfigurationProvider' for {plugin} and {connector}.");
                        var mongodbSinkConfig =
                            configurationProvider.GetSinkConfigProperties<MongoSinkConfig>(connector.Name, plugin);
                        if (mongodbSinkConfig == null)
                            throw new InvalidOperationException(
                                $@"Unable to find the configuration matching {plugin} and {connector}.");
                        var settings = MongoClientSettings.FromConnectionString(mongodbSinkConfig.ConnectionUri);
                        settings.ApplicationName = $"{connector.Name}-{taskId:00}";
                        return new MongoClient(settings);
                    });
                }
            }
        }
        
        protected abstract void AddAdditionalServices(IServiceCollection collection, IConfiguration configuration);
    }
}