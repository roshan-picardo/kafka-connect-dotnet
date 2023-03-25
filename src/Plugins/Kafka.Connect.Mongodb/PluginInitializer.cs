using System;
using System.Collections.Generic;
using Kafka.Connect.Mongodb.Collections;
using Kafka.Connect.Mongodb.Models;
using Kafka.Connect.Mongodb.Strategies;
using Kafka.Connect.Plugin;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;

namespace Kafka.Connect.Mongodb
{
    public abstract class PluginInitializer : IPluginInitializer
    {
        public void AddServices(IServiceCollection collection, IConfiguration configuration, string plugin, IEnumerable<string> connectors)
        {
            try
            {
                collection
                    .AddScoped<ISinkHandler>(provider => new MongodbSinkHandler(
                        provider.GetService<ILogger<MongodbSinkHandler>>(),
                        provider.GetService<IEnumerable<IWriteModelStrategyProvider>>(),
                        provider.GetService<Plugin.Providers.IConfigurationProvider>(),
                        provider.GetService<IMongoWriter>(), plugin))
                    .AddScoped<IPluginInitializer, DefaultPluginInitializer>()
                    .AddScoped<IMongoClientProvider, MongoClientProvider>()
                    .AddScoped<IWriteModelStrategyProvider, WriteModelStrategyProvider>()
                    .AddScoped<IWriteModelStrategyProvider, TopicWriteModelStrategyProvider>()
                    .AddScoped<IWriteModelStrategy, DefaultWriteModelStrategy>()
                    .AddScoped<IWriteModelStrategy, TopicSkipWriteModelStrategy>()
                    .AddScoped<IMongoWriter, MongoWriter>();
                AddMongoClients(collection, plugin, connectors);
                AddAdditionalServices(collection, configuration);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
        
        private void AddMongoClients(IServiceCollection collection, string plugin, IEnumerable<string> connectors)
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