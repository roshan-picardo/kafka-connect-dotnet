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

public class DefaultPluginInitializer : IPluginInitializer
{
    public void AddServices(
        IServiceCollection collection,
        IConfiguration configuration,
        params (string Name, int Tasks)[] connectors)
    {
        collection
            .AddScoped<IMongoCommandHandler, MongoCommandHandler>()
            .AddScoped<IPluginHandler, MongoPluginHandler>()
            .AddScoped<IPluginInitializer, DefaultPluginInitializer>()
            .AddSingleton<IMongoClientProvider, MongoClientProvider>()
            .AddScoped<IStrategy, ReadStrategy>()
            .AddScoped<IStrategy, DeleteStrategy>()
            .AddScoped<IStrategy, InsertStrategy>()
            .AddScoped<IStrategy, UpdateStrategy>()
            .AddScoped<IStrategy, UpsertStrategy>()
            .AddScoped<IStrategy, StreamsReadStrategy>()
            .AddScoped<IMongoQueryRunner, MongoQueryRunner>();
    }
}