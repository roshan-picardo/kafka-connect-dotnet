using System;
using System.Collections.Generic;
using System.Linq;
using Kafka.Connect.Mongodb.Collections;
using Kafka.Connect.Mongodb.Extensions;
using Kafka.Connect.Mongodb.Models;
using Kafka.Connect.Mongodb.Strategies;
using Kafka.Connect.Plugin;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace Kafka.Connect.Mongodb
{
    public abstract class PluginInitializer : IPluginInitializer
    {
        public void AddServices(IServiceCollection collection, IConfiguration configuration, string plugin)
        {
            try
            {
                collection
                    .AddScoped<ISinkHandler>(provider => new MongodbSinkHandler(
                        provider.GetService<ILogger<MongodbSinkHandler>>(),
                        provider.GetService<IEnumerable<IWriteModelStrategyProvider>>(),
                        provider.GetService<IMongoSinkConfigProvider>(),
                        provider.GetService<IMongoWriter>(), plugin))
                    .AddScoped<IPluginInitializer, DefaultPluginInitializer>()
                    .AddScoped<IMongoClientProvider, MongoClientProvider>()
                    .AddScoped<IWriteModelStrategyProvider, WriteModelStrategyProvider>()
                    .AddScoped<IWriteModelStrategyProvider, TopicWriteModelStrategyProvider>()
                    .AddScoped<IWriteModelStrategy, DefaultWriteModelStrategy>()
                    .AddScoped<IWriteModelStrategy, TopicSkipWriteModelStrategy>()
                    .AddScoped<IMongoWriter, MongoWriter>()
                    .AddScoped<IMongoSinkConfigProvider, MongoSinkConfigProvider>();
                AddMongoClients(collection, configuration, plugin);
                AddMoreServices(collection, configuration, plugin);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
        
        
        private static void AddMongoClients(IServiceCollection collection, IConfiguration configuration, string plugin)
        {
            var mongoSinkConfigs =
                configuration.GetSection("worker:connectors").Get<List<SinkConfig<MongoSinkConfig>>>();
            var workerConfig = configuration.GetSection("worker").Get<SinkConfig<MongoSinkConfig>>();
            
            foreach (var mongoSinkConfig in mongoSinkConfigs.Merged(workerConfig).Where(c => c.Plugin == plugin))
            {
                collection.AddSingleton<IMongoClient>(provider =>
                {
                    var settings =
                        MongoClientSettings.FromConnectionString(mongoSinkConfig.Sink.Properties.ConnectionUri);
                    settings.ApplicationName = mongoSinkConfig.Name;
                    return new MongoClient(settings);
                });
            }
        }
        
        protected abstract void AddMoreServices(IServiceCollection collection, IConfiguration configuration, string plugin);
    }
}