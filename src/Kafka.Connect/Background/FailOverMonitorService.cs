using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Connect.Builders;
using Kafka.Connect.Configurations;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Tokens;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog.Context;

namespace Kafka.Connect.Background
{
    public class FailOverMonitorService : BackgroundService
    {
        private readonly ILogger<FailOverMonitorService> _logger;
        private readonly IWorker _worker;
        private readonly ITokenHandler _tokenHandler;
        private readonly IConfigurationProvider _configProvider;
        private readonly IKafkaClientBuilder _kafkaClientBuilder;

        public FailOverMonitorService(ILogger<FailOverMonitorService> logger, IWorker worker,
            IServiceScopeFactory serviceScopeFactory, ITokenHandler tokenHandler, IConfigurationProvider configProvider)
        {
            _logger = logger;
            _worker = worker;
            _tokenHandler = tokenHandler;
            _configProvider = configProvider;
            _kafkaClientBuilder = serviceScopeFactory.CreateScope().ServiceProvider.GetService<IKafkaClientBuilder>();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var failOverConfig = _configProvider.GetFailOverConfig();
            var connectorConfigs = _configProvider.GetConnectorConfigs();
            try
            {
                if (!failOverConfig.Disabled)
                {
                    _logger.LogDebug("{@Log}", new {Message = "Starting fail over monitoring service..."});
                    await Task.Delay(failOverConfig.InitialDelayMs, stoppingToken);
                    var adminClient = _logger.Timed("Creating Kafka admin client")
                        .Execute(() => _kafkaClientBuilder.GetAdminClient(_configProvider.GetConsumerConfig()));
                    
                    var thresholds = connectorConfigs.Where(c => !c.Disabled)
                        .ToDictionary(c => c.Name, _ => failOverConfig.FailureThreshold);
                    
                    while (!stoppingToken.IsCancellationRequested)
                    {
                        await Task.Delay(failOverConfig.PeriodicDelayMs, stoppingToken);
                        foreach (var connector in connectorConfigs)
                        {
                            using (LogContext.PushProperty("Connector", connector.Name))
                            {
                                var topics = connector.Topics ?? new List<string>();
                                if (!string.IsNullOrWhiteSpace(connector.Topic) && !topics.Contains(connector.Topic))
                                {
                                    topics.Add(connector.Topic);
                                }

                                try
                                {
                                    var metadata = topics.Select(topic =>
                                    {
                                        using (LogContext.PushProperty("Topic", topic))
                                        {
                                            return _logger.Timed("Obtaining the topic metadata.")
                                                .Execute(() => adminClient.GetMetadata(topic, TimeSpan.FromSeconds(2)));
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
                                        _logger.LogTrace("{@Log}", new
                                        {
                                            Message = "Broker failure detected.",
                                            Connector = connector.Name,
                                            Threshold = thresholds[connector.Name]
                                        });
                                    }
                                }
                                catch (Exception ex)
                                {
                                    thresholds[connector.Name]--;
                                    _logger.LogError(ex, "{@Log}",
                                        new
                                        {
                                            Message = "Unhandled error while reading metadata.",
                                            Connector = connector.Name,
                                            Threshold = thresholds[connector.Name]
                                        });
                                    _logger.LogTrace("{@Log}", new
                                    {
                                        Message = "Broker failure detected.",
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
                                await _logger.Timed("Broker failure detected. Restarting the worker.")
                                    .Execute(async () => await _worker.RestartAsync(failOverConfig.RestartDelayMs));
                            }
                            else
                            {
                                foreach (var connector in thresholds.Where(t=>t.Value <= 0)
                                    .Select(t => new {Name = t.Key, Connector = _worker.GetConnector(t.Key)})
                                    .Where(c => c.Connector != null))
                                {
                                    using (LogContext.PushProperty("Connector", connector.Name))
                                    {
                                        await _logger
                                            .Timed("Broker failure detected. Restarting the connector.")
                                            .Execute(async () =>
                                                await connector.Connector.Restart(failOverConfig.RestartDelayMs, null));
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
                    _logger.LogTrace("{@Log}", new {Message = "Task has been cancelled. Fail over service will be terminated."});
                }
                else
                {
                    _logger.LogError(ex, "{@Log}", new { Message = "Fail over monitoring service reported errors / hasn't started." });
                }
            }
            finally
            {
                _logger.LogDebug("{@Log}",
                    failOverConfig.Disabled
                        ? new {Message = "Fail over monitoring service is not enabled..."}
                        :  new {Message = "Stopping fail over monitoring service..."});
            }
        }
    }
}