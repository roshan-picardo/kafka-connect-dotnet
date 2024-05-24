using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Tokens;
using Microsoft.Extensions.DependencyInjection;
using Serilog.Context;
using IConfigurationProvider = Kafka.Connect.Providers.IConfigurationProvider;

namespace Kafka.Connect;

public class Worker(
    ILogger<Worker> logger,
    IServiceScopeFactory serviceScopeFactory,
    IExecutionContext executionContext,
    IConfigurationProvider configurationProvider)
    : IWorker
{
    private PauseTokenSource _pauseTokenSource;
    private readonly IList<Task> _tasks = new List<Task>();

    public bool IsPaused => _pauseTokenSource.IsPaused;
    public bool IsStopped { get; private set; }

    public async Task Execute(CancellationTokenSource cts)
    {
        if(!configurationProvider.IsWorker) return;
        configurationProvider.Validate();
        logger.Debug("Starting the worker.");
        executionContext.Initialize(configurationProvider.GetNodeName(), this);

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
                logger.Debug("Starting connectors.");
                foreach (var connector in configurationProvider.GetAllConnectorConfigs())
                {
                    AddConnectorTask(connector.Name, cts.Token);
                }

                do
                {
                    await Task.WhenAll(_tasks).ContinueWith(t =>
                    {
                        if (t is { IsFaulted: true, IsCanceled: false })
                        {
                            logger.Error("Worker is faulted, and is terminated.", t.Exception?.InnerException);
                        }

                    }, CancellationToken.None);
                } while (!_tasks.Any(t => t.IsCompleted));
            }
            catch (Exception ex)
            {
                logger.Error("Worker is faulted, and is terminated.", ex);
            }

            if (cts.IsCancellationRequested || _pauseTokenSource.IsPaused) continue;
                
            if(! await executionContext.Retry()) _pauseTokenSource.Pause();
        }

        IsStopped = true;
        logger.Debug("Shutting down the worker.");
    }

    private void AddConnectorTask(string name, CancellationToken token)
    {
        using (ConnectLog.Connector(name))
        {
            using var scope = serviceScopeFactory.CreateScope();
            var connector = scope.ServiceProvider.GetService<IConnector>();
            if (connector == null)
            {
                logger.Warning("Unable to load and terminating the connector.");
                return;
            }

            logger.Error("Connector Starting.");
            var linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(token);
            _pauseTokenSource.AddLinkedTokenSource(linkedTokenSource);
            var connectorTask = connector.Execute(name, linkedTokenSource).ContinueWith(
                t =>
                {
                    if (t is { IsFaulted: true, IsCanceled: false })
                    {
                        logger.Error("Connector is faulted, and is terminated.", t.Exception?.InnerException);
                    }

                    logger.Debug("Connector Stopped.");
                }, CancellationToken.None);
            _tasks.Add(connectorTask);
        }
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