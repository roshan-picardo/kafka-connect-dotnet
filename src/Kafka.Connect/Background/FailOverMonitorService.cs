using System;
using System.Collections.Generic;
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
using Serilog.Context;

namespace Kafka.Connect.Background;

public class FailOverMonitorService : BackgroundService
{
    private readonly ILogger<FailOverMonitorService> _logger;
    private readonly IExecutionContext _executionContext;
    private readonly ITokenHandler _tokenHandler;
    private readonly IConfigurationProvider _configurationProvider;
    private readonly IKafkaClientBuilder _kafkaClientBuilder;

    public FailOverMonitorService(ILogger<FailOverMonitorService> logger, IExecutionContext executionContext,
        IServiceScopeFactory serviceScopeFactory, ITokenHandler tokenHandler, IConfigurationProvider configurationProvider)
    {
        _logger = logger;
        _executionContext = executionContext;
        _tokenHandler = tokenHandler;
        _configurationProvider = configurationProvider;
        _kafkaClientBuilder = serviceScopeFactory.CreateScope().ServiceProvider.GetService<IKafkaClientBuilder>();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var failOverConfig = _configurationProvider.GetFailOverConfig();
        var connectorConfigs = _configurationProvider.GetAllConnectorConfigs();
        try
        {
            if (!failOverConfig.Disabled)
            {
                _logger.Debug("Starting fail over monitoring service...");
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
                            var topics = connector.Topics ?? new List<string>();
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
                                    _logger.Trace("Broker failure detected.",  data:new { Connector = connector.Name, Threshold = thresholds[connector.Name] });
                                }
                            }
                            catch (Exception ex)
                            {
                                thresholds[connector.Name]--;
                                _logger.Error( "Unhandled error while reading metadata.", 
                                    new
                                    {
                                        Connector = connector.Name,
                                        Threshold = thresholds[connector.Name]
                                    }, ex);
                                _logger.Trace("Broker failure detected.", new
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
                            await _executionContext.Restart(failOverConfig.RestartDelayMs);
                        }
                        else
                        {
                            foreach (var connector in thresholds.Where(t=>t.Value <= 0)
                                         .Select(t => new {Name = t.Key, Connector = _executionContext.GetConnector(t.Key)})
                                         .Where(c => c.Connector != null))
                            {
                                using (ConnectLog.Connector(connector.Name))
                                {
                                    await _executionContext.Restart(failOverConfig.RestartDelayMs, connector.Name);
                                }
                            }
                        }
                        thresholds = connectorConfigs.Where(c => !c.Disabled)
                            .ToDictionary(c => c.Name, _ => failOverConfig.FailureThreshold);
                    }

                    _tokenHandler.DoNothing();
                }
            }
        }
        catch (Exception ex)
        {
            if (ex is TaskCanceledException or OperationCanceledException)
            {
                _logger.Trace("Task has been cancelled. Fail over service will be terminated.");
            }
            else
            {
                _logger.Error("Fail over monitoring service reported errors / hasn't started.", ex);
            }
        }
        finally
        {
            _logger.Debug(failOverConfig.Disabled ? "Fail over monitoring service is not enabled..." :  "Stopping fail over monitoring service...");
        }
    }
}