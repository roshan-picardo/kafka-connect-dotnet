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
        public void AddServices(IServiceCollection collection, IConfiguration configuration, (string Plugin, IEnumerable<string> Connectors) pluginConfig)
        {
            try
            {
                collection
                    .AddScoped<ISinkHandler, MongoSinkHandler>()
                    .AddScoped<IPluginInitializer, DefaultPluginInitializer>()
                    .AddScoped<IMongoClientProvider, MongoClientProvider>()
                    //.AddScoped<IWriteModelStrategyProvider, WriteModelStrategyProvider>()
                    //.AddScoped<IWriteModelStrategyProvider, TopicWriteModelStrategyProvider>()
                    //.AddScoped<IWriteStrategy, DefaultWriteModelStrategy>()
                    .AddScoped<IWriteStrategy, DefaultWriteModelStrategy>()
                    .AddScoped<IMongoWriter, MongoWriter>();
                AddMongoClients(collection, pluginConfig.Plugin, pluginConfig.Connectors);
                AddAdditionalServices(collection, configuration);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
        
        private static void AddMongoClients(IServiceCollection collection, string plugin, IEnumerable<string> connectors)
        {
            foreach (var connector in connectors)
            {
                collection.AddSingleton<IMongoClient>(provider =>
                {
                    var configurationProvider = provider.GetService<Plugin.Providers.IConfigurationProvider>() ?? throw new InvalidOperationException($@"Unable to resolve service for type 'IConfigurationProvider' for {plugin} and {connector}.");
                    var mongodbSinkConfig = configurationProvider.GetSinkConfigProperties<MongoSinkConfig>(connector, plugin);
                    if (mongodbSinkConfig == null) throw new InvalidOperationException($@"Unable to find the configuration matching {plugin} and {connector}.");
                    var settings = MongoClientSettings.FromConnectionString(mongodbSinkConfig.ConnectionUri);
                    settings.ApplicationName = connector;
                    return new MongoClient(settings);
                });
            }
        }
        
        protected abstract void AddAdditionalServices(IServiceCollection collection, IConfiguration configuration);
    }
}