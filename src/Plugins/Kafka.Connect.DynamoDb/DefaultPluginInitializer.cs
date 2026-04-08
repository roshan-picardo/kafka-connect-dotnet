using System;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.Runtime;
using Kafka.Connect.DynamoDb.Collections;
using Kafka.Connect.DynamoDb.Models;
using Kafka.Connect.DynamoDb.Strategies;
using Kafka.Connect.Plugin;
using Kafka.Connect.Plugin.Strategies;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Kafka.Connect.DynamoDb;

public class DefaultPluginInitializer : IPluginInitializer
{
    public void AddServices(
        IServiceCollection collection,
        IConfiguration configuration,
        params (string Name, int Tasks)[] connectors)
    {
        collection
            .AddScoped<IDynamoDbCommandHandler, DynamoDbCommandHandler>()
            .AddScoped<IPluginHandler, DynamoDbPluginHandler>()
            .AddScoped<IPluginInitializer, DefaultPluginInitializer>()
            .AddSingleton<IDynamoDbClientProvider, DynamoDbClientProvider>()
            .AddScoped<IStrategy, ReadStrategy>()
            .AddScoped<IStrategy, StreamReadStrategy>()
            .AddScoped<IStrategy, DeleteStrategy>()
            .AddScoped<IStrategy, InsertStrategy>()
            .AddScoped<IStrategy, UpdateStrategy>()
            .AddScoped<IStrategy, UpsertStrategy>()
            .AddScoped<IDynamoDbQueryRunner, DynamoDbQueryRunner>();
    }
}
