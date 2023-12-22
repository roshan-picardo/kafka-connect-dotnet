using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Admin;
using Kafka.Connect.Builders;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Providers;
using Kafka.Connect.Tokens;
using Microsoft.Extensions.DependencyInjection;
using Serilog.Context;

namespace Kafka.Connect;

public class Worker : IWorker
{
    private readonly ILogger<Worker> _logger;
    private readonly IServiceScopeFactory _serviceScopeFactory;
    private readonly IExecutionContext _executionContext;
    private readonly IConfigurationProvider _configurationProvider;
    private readonly IKafkaClientBuilder _kafkaClientBuilder;
    private PauseTokenSource _pauseTokenSource;

    public Worker(ILogger<Worker> logger, IServiceScopeFactory serviceScopeFactory,
        IExecutionContext executionContext, IConfigurationProvider configurationProvider, IKafkaClientBuilder kafkaClientBuilder)
    {
        _logger = logger;
        _serviceScopeFactory = serviceScopeFactory;
        _executionContext = executionContext;
        _configurationProvider = configurationProvider;
        _kafkaClientBuilder = kafkaClientBuilder;
    }

    public bool IsPaused => _pauseTokenSource.IsPaused;
    public bool IsStopped { get; private set; }


    public async Task Execute(CancellationTokenSource cts)
    {
        _configurationProvider.Validate();
        _logger.Debug("Starting the worker.");
        _executionContext.Initialize(_configurationProvider.GetWorkerName(), this);

        _pauseTokenSource = PauseTokenSource.New();
        while (!cts.IsCancellationRequested)
        {
            await _pauseTokenSource.Token.WaitWhilePausedAsync(cts.Token);
            if (cts.IsCancellationRequested)
            {
                break;
            }
            try
            {
                var allConnectorConfigs = _configurationProvider.GetAllConnectorConfigs();
                var adminClient = _kafkaClientBuilder.GetAdminClient();
                
                // create the source topic if required
                await Parallel.ForEachAsync(allConnectorConfigs.Where(c => c.Type == ConnectorType.Source),  async (config, _) =>
                {
                    var meta = adminClient.GetMetadata(config.Topic, TimeSpan.FromSeconds(2));
                    if (!meta.Topics.Exists(t => t.Topic == config.Topic  && !t.Error.IsError && t.Partitions.Count > 0))
                    {
                        try
                        {
                            await adminClient.CreateTopicsAsync(new[]
                            {
                                new TopicSpecification
                                {
                                    Name = config.Topic, 
                                    NumPartitions = 50,
                                    ReplicationFactor = (short)(meta.Brokers.Count > 3 ? 3 : meta.Brokers.Count)
                                }
                            });
                        }
                        catch (Exception)
                        {
                            //
                        }
                    }
                });
                
                _logger.Debug("Starting connectors.", new { allConnectorConfigs.Count});
                var connectors = from job in allConnectorConfigs.Select(s =>
                        new { s.Name, Scope = _serviceScopeFactory.CreateScope()})
                    let connector = job.Scope.ServiceProvider.GetService<IConnector>()
                    select new {Connector = connector, job.Name};
                await Task.WhenAll(connectors.Select(connector =>
                    {
                        using (LogContext.PushProperty("Connector", connector.Name))
                        {
                            if (connector.Connector == null)
                            {
                                _logger.Warning("Unable to load and terminating the connector.");
                                return Task.CompletedTask;
                            }
                            _logger.Trace("Connector Starting.");
                            var linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cts.Token);
                            _pauseTokenSource.AddLinkedTokenSource(linkedTokenSource);
                            var connectorTask = connector.Connector.Execute(connector.Name, linkedTokenSource).ContinueWith(
                                t =>
                                {
                                    if (t.IsFaulted && !t.IsCanceled)
                                    {
                                        _logger.Error("Connector is faulted, and is terminated.", t.Exception?.InnerException);
                                    }

                                    _logger.Debug("Connector Stopped.");
                                }, CancellationToken.None);
                            return connectorTask;
                        }
                    }))
                    .ContinueWith(t =>
                    {
                        if (t.IsFaulted && !t.IsCanceled)
                        {
                            _logger.Error( "Worker is faulted, and is terminated.", t.Exception?.InnerException);
                        }

                    }, CancellationToken.None);
            }
            catch (Exception ex)
            {
                _logger.Error("Worker is faulted, and is terminated.", ex);
            }

            if (cts.IsCancellationRequested || _pauseTokenSource.IsPaused) continue;
                
            if(! await _executionContext.Retry()) _pauseTokenSource.Pause();
        }

        IsStopped = true;
        _logger.Debug("Shutting down the worker.");
    }

    public Task Pause()
    {
        _pauseTokenSource.Resume();
        return Task.CompletedTask;
    }

    public Task Resume()
    {
        _pauseTokenSource.Resume();
        return Task.CompletedTask;
    }
        
}