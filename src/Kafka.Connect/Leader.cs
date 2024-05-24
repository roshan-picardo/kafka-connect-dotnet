using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Admin;
using Kafka.Connect.Builders;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Providers;
using Microsoft.Extensions.DependencyInjection;
using Serilog.Context;

namespace Kafka.Connect;

public class Leader(
    IConfigurationProvider configurationProvider,
    IExecutionContext executionContext,
    IKafkaClientBuilder kafkaClientBuilder,
    IServiceScopeFactory serviceScopeFactory,
    ILogger<Leader> logger) : ILeader
{
    public Task Pause()
    {
        throw new NotImplementedException();
    }

    public Task Resume()
    {
        throw new NotImplementedException();
    }

    public async Task Execute(CancellationTokenSource cts)
    {
        if (!configurationProvider.IsLeader) return;
        logger.Debug("Starting the leader.");
        executionContext.Initialize(configurationProvider.GetNodeName(), this);

        while (!cts.IsCancellationRequested)
        {
            var leaderConfig = configurationProvider.GetLeaderConfig();
            await CreateInternalTopics(leaderConfig.Topics);
            
             try
            {
                var allConnectorConfigs = new List<ConnectorConfig> { leaderConfig.Connector };
                logger.Debug("Starting connectors.", new { allConnectorConfigs.Count});
                var connectors = from job in allConnectorConfigs.Select(s =>
                        new { s.Name, Scope = serviceScopeFactory.CreateScope()})
                    let connector = job.Scope.ServiceProvider.GetService<IConnector>()
                    select new {Connector = connector, job.Name};
                await Task.WhenAll(connectors.Select(connector =>
                    {
                        using (ConnectLog.Connector(connector.Name))
                        {
                            if (connector.Connector == null)
                            {
                                logger.Warning("Unable to load and terminating the connector.");
                                return Task.CompletedTask;
                            }
                            logger.Trace("Connector Starting.");
                            var linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cts.Token);
                            //_pauseTokenSource.AddLinkedTokenSource(linkedTokenSource);
                            var connectorTask = connector.Connector.Execute(connector.Name, linkedTokenSource).ContinueWith(
                                t =>
                                {
                                    if (t is { IsFaulted: true, IsCanceled: false })
                                    {
                                        logger.Error("Connector is faulted, and is terminated.", t.Exception?.InnerException);
                                    }

                                    logger.Debug("Connector Stopped.");
                                }, CancellationToken.None);
                            return connectorTask;
                        }
                    }))
                    .ContinueWith(t =>
                    {
                        if (t is { IsFaulted: true, IsCanceled: false })
                        {
                            logger.Error( "Leader is faulted, and is terminated.", t.Exception?.InnerException);
                        }

                    }, CancellationToken.None);
            }
            catch (Exception ex)
            {
                logger.Error("Leader is faulted, and is terminated.", ex);
            }
        }
    }

    private async Task CreateInternalTopics(InternalTopicConfig topics)
    {
        var adminClient = kafkaClientBuilder.GetAdminClient();
        var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(20));
        var replication = (short)(metadata.Brokers.Count > 3 ? 3 : metadata.Brokers.Count);
        var topicSpecs = new List<TopicSpecification>();
        if (!metadata.Topics.Exists(t => t.Topic == topics.Config))
        {
            topicSpecs.Add(new() { Name = topics.Config, NumPartitions = 1, ReplicationFactor = replication });
        }

        if (!metadata.Topics.Exists(t => t.Topic == topics.Command))
        {
            topicSpecs.Add(new() { Name = topics.Command, NumPartitions = 50, ReplicationFactor = replication });
        }

        if (topicSpecs.Count > 0)
        {
            await adminClient.CreateTopicsAsync(topicSpecs);
        }
    }

    public bool IsPaused { get; private set; }
    public bool IsStopped { get; private set; }
}