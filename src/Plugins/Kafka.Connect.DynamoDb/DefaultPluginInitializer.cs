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
            .AddScoped<IDynamoDbClientProvider, DynamoDbClientProvider>()
            .AddScoped<IStrategy, ReadStrategy>()
            .AddScoped<IStrategy, StreamReadStrategy>()
            .AddScoped<IStrategy, DeleteStrategy>()
            .AddScoped<IStrategy, InsertStrategy>()
            .AddScoped<IStrategy, UpdateStrategy>()
            .AddScoped<IStrategy, UpsertStrategy>()
            .AddScoped<IDynamoDbQueryRunner, DynamoDbQueryRunner>();
        
        AddDynamoDbClients(collection, connectors);
    }

    private static void AddDynamoDbClients(IServiceCollection collection, params (string Name, int Tasks)[] connectors)
    {
        foreach (var connector in connectors)
        {
            for (var t = 0; t < connector.Tasks; t++)
            {
                var taskId = t + 1;
                collection.AddSingleton<IAmazonDynamoDB>(provider =>
                {
                    var configurationProvider = provider.GetService<Plugin.Providers.IConfigurationProvider>() ??
                                                throw new InvalidOperationException(
                                                    $@"Unable to resolve service for type 'IConfigurationProvider' for {connector}.");
                    
                    var pluginConfig = configurationProvider.GetPluginConfig<PluginConfig>(connector.Name);
                    if (pluginConfig == null)
                        throw new InvalidOperationException(
                            $"Unable to find the configuration matching {connector.Name}.");

                    var config = new AmazonDynamoDBConfig();
                    
                    if (!string.IsNullOrEmpty(pluginConfig.Region))
                    {
                        config.RegionEndpoint = RegionEndpoint.GetBySystemName(pluginConfig.Region);
                    }
                    
                    if (!string.IsNullOrEmpty(pluginConfig.ServiceUrl))
                    {
                        config.ServiceURL = pluginConfig.ServiceUrl;
                    }

                    IAmazonDynamoDB client;
                    
                    if (!string.IsNullOrEmpty(pluginConfig.AccessKeyId) &&
                        !string.IsNullOrEmpty(pluginConfig.SecretAccessKey))
                    {
                        var credentials = new BasicAWSCredentials(
                            pluginConfig.AccessKeyId,
                            pluginConfig.SecretAccessKey);
                        client = new AmazonDynamoDBClient(credentials, config);
                    }
                    else
                    {
                        // Use default credentials (IAM role, environment variables, etc.)
                        client = new AmazonDynamoDBClient(config);
                    }

                    return client;
                });
                
                // Register DynamoDB Streams client
                collection.AddSingleton<AmazonDynamoDBStreamsClient>(provider =>
                {
                    var configurationProvider = provider.GetService<Plugin.Providers.IConfigurationProvider>() ??
                                                throw new InvalidOperationException(
                                                    $@"Unable to resolve service for type 'IConfigurationProvider' for {connector}.");
                    
                    var pluginConfig = configurationProvider.GetPluginConfig<PluginConfig>(connector.Name);
                    if (pluginConfig == null)
                        throw new InvalidOperationException(
                            $"Unable to find the configuration matching {connector.Name}.");

                    var config = new AmazonDynamoDBStreamsConfig();
                    
                    if (!string.IsNullOrEmpty(pluginConfig.Region))
                    {
                        config.RegionEndpoint = RegionEndpoint.GetBySystemName(pluginConfig.Region);
                    }
                    
                    if (!string.IsNullOrEmpty(pluginConfig.ServiceUrl))
                    {
                        config.ServiceURL = pluginConfig.ServiceUrl;
                    }

                    AmazonDynamoDBStreamsClient client;
                    
                    if (!string.IsNullOrEmpty(pluginConfig.AccessKeyId) &&
                        !string.IsNullOrEmpty(pluginConfig.SecretAccessKey))
                    {
                        var credentials = new BasicAWSCredentials(
                            pluginConfig.AccessKeyId,
                            pluginConfig.SecretAccessKey);
                        client = new AmazonDynamoDBStreamsClient(credentials, config);
                    }
                    else
                    {
                        // Use default credentials (IAM role, environment variables, etc.)
                        client = new AmazonDynamoDBStreamsClient(config);
                    }

                    return client;
                });
            }
        }
    }
}
