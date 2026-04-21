using System;
using System.Collections.Concurrent;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.Runtime;
using Kafka.Connect.DynamoDb.Models;

namespace Kafka.Connect.DynamoDb.Collections;

public class DynamoDbClientProvider(Plugin.Providers.IConfigurationProvider configurationProvider) : IDynamoDbClientProvider
{
    private readonly ConcurrentDictionary<string, IAmazonDynamoDB> _dynamoDbClientCache = new();
    private readonly ConcurrentDictionary<string, AmazonDynamoDBStreamsClient> _streamsClientCache = new();
    
    public IAmazonDynamoDB GetDynamoDbClient(string connector, int taskId)
    {
        var key = $"{connector}-{taskId:00}";
        
        return _dynamoDbClientCache.GetOrAdd(key, _ =>
        {
            var pluginConfig = configurationProvider.GetPluginConfig<PluginConfig>(connector);
            if (pluginConfig == null)
                throw new InvalidOperationException($"Unable to find the configuration matching {connector}.");

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
                client = new AmazonDynamoDBClient(config);
            }

            return client;
        });
    }
    
    public AmazonDynamoDBStreamsClient GetStreamsClient(string connector, int taskId)
    {
        var key = $"{connector}-{taskId:00}";
        
        return _streamsClientCache.GetOrAdd(key, _ =>
        {
            var pluginConfig = configurationProvider.GetPluginConfig<PluginConfig>(connector);
            if (pluginConfig == null)
                throw new InvalidOperationException($"Unable to find the configuration matching {connector}.");

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
                client = new AmazonDynamoDBStreamsClient(config);
            }

            return client;
        });
    }
}
