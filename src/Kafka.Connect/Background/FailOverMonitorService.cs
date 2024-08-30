using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Builders;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Tokens;
using Kafka.Connect.Providers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;


namespace Kafka.Connect.Background;

public class FailOverMonitorService(
    ILogger<FailOverMonitorService> logger,
    IExecutionContext executionContext,
    IServiceScopeFactory serviceScopeFactory,
    ITokenHandler tokenHandler,
    IConfigurationProvider configurationProvider)
    : BackgroundService
{
    private readonly IKafkaClientBuilder _kafkaClientBuilder = serviceScopeFactory.CreateScope().ServiceProvider.GetService<IKafkaClientBuilder>();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var failOverConfig = configurationProvider.GetFailOverConfig();
        var connectorConfigs = configurationProvider.GetAllConnectorConfigs();
        try
        {
            if (!failOverConfig.Disabled)
            {
                logger.Debug("Starting fail over monitoring service...");
                await Task.Delay(failOverConfig.InitialDelayMs, stoppingToken);
                var adminClient =  _kafkaClientBuilder.GetAdminClient();
                    
                var thresholds = connectorConfigs.Where(c => !c.Disabled)
                    .ToDictionary(c => c.Name, _ => failOverConfig.FailureThreshold);
                    
                while (!stoppingToken.IsCancellationRequested)
                {
                    await Task.Delay(failOverConfig.PeriodicDelayMs, stoppingToken);
                    foreach (var connector in connectorConfigs)
                    {
                        using (ConnectLog.Connector(connector.Name))
                        {
                            var topics = connector.Topics?.Select(t => t.Key).ToList() ?? [];
                            try
                            {
                                var metadata = topics.Select(topic =>
                                {
                                    using (ConnectLog.TopicPartitionOffset(topic))
                                    {
                                        // TODO: how to time this?
                                        return adminClient.GetMetadata(topic, TimeSpan.FromSeconds(2));
                                    }
                                }).ToList();

                                if (metadata.All(m => m == null || m.Topics.All(t => !topics.Contains(t.Topic))))
                                {
                                    continue;
                                }

                                if (metadata
                                    .Where(m => m != null)
                                    .SelectMany(m => m.Topics)
                                    .All(t => t.Error == ErrorCode.NoError))
                                {
                                    thresholds[connector.Name] = failOverConfig.FailureThreshold;
                                }
                                else
                                {
                                    thresholds[connector.Name]--;
                                    logger.Trace("Broker failure detected.",  data:new { Connector = connector.Name, Threshold = thresholds[connector.Name] });
                                }
                            }
                            catch (Exception ex)
                            {
                                thresholds[connector.Name]--;
                                logger.Error( "Unhandled error while reading metadata.", 
                                    new
                                    {
                                        Connector = connector.Name,
                                        Threshold = thresholds[connector.Name]
                                    }, ex);
                                logger.Trace("Broker failure detected.", new
                                {
                                    Connector = connector.Name,
                                    Threshold = thresholds[connector.Name]
                                });
                            }
                        }
                    }

                    if (thresholds.Any(t => t.Value <= 0))
                    {
                        if (thresholds.All(t => t.Value <= 0))
                        {
                            await executionContext.Restart(failOverConfig.RestartDelayMs);
                        }
                        else
                        {
                            foreach (var connector in thresholds.Where(t=>t.Value <= 0)
                                         .Select(t => new {Name = t.Key, Connector = executionContext.GetConnector(t.Key)})
                                         .Where(c => c.Connector != null))
                            {
                                using (ConnectLog.Connector(connector.Name))
                                {
                                    await executionContext.Restart(failOverConfig.RestartDelayMs, connector.Name);
                                }
                            }
                        }
                        thresholds = connectorConfigs.Where(c => !c.Disabled)
                            .ToDictionary(c => c.Name, _ => failOverConfig.FailureThreshold);
                    }

                    tokenHandler.NoOp();
                }
            }
        }
        catch (Exception ex)
        {
            if (ex is TaskCanceledException or OperationCanceledException)
            {
                logger.Trace("Task has been cancelled. Fail over service will be terminated.");
            }
            else
            {
                logger.Error("Fail over monitoring service reported errors / hasn't started.", ex);
            }
        }
        finally
        {
            logger.Debug(failOverConfig.Disabled ? "Fail over monitoring service is not enabled..." :  "Stopping fail over monitoring service...");
        }
    }
}