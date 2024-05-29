using System;
using Kafka.Connect.MongoDb.Collections;
using Kafka.Connect.MongoDb.Models;
using Kafka.Connect.MongoDb.Strategies;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Strategies;
using Microsoft.Extensions.DependencyInjection;
using MongoDB.Driver;
using Microsoft.Extensions.Configuration;

namespace Kafka.Connect.MongoDb;

public class DefaultPluginInitializer : PluginInitializer
{
    public override void AddServices(IServiceCollection collection, IConfiguration configuration, params (string Name, int Tasks)[] connectors)
    {
        collection
            .AddScoped<ISinkHandler, MongoSinkHandler>()
            .AddScoped<IPluginInitializer, DefaultPluginInitializer>()
            .AddScoped<IMongoClientProvider, MongoClientProvider>()
            .AddScoped<IQueryStrategy, ReadStrategy>()
            .AddScoped<IQueryStrategy, DeleteStrategy>()
            .AddScoped<IQueryStrategy, InsertStrategy>()
            .AddScoped<IQueryStrategy, UpdateStrategy>()
            .AddScoped<IQueryStrategy, UpsertStrategy>()
            .AddScoped<IMongoQueryRunner, MongoQueryRunner>()
            .AddScoped<ISourceHandler, MongoSourceHandler>();
        AddMongoClients(collection, connectors);
    }

    private static void AddMongoClients(IServiceCollection collection, params (string Name, int Tasks)[] connectors)
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
                                                    $@"Unable to resolve service for type 'IConfigurationProvider' for {connector}.");
                    var pluginConfig = configurationProvider.GetPluginConfig<PluginConfig>(connector.Name);
                    if (pluginConfig == null)
                        throw new InvalidOperationException(
                            $"Unable to find the configuration matching {connector.Name}.");
                    var settings = MongoClientSettings.FromConnectionString(pluginConfig.ConnectionUri);
                    settings.ApplicationName = $"{connector.Name}-{taskId:00}";
                    return new MongoClient(settings);
                });
            }
        }
    }
}