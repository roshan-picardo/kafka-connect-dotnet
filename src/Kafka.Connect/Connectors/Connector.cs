using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Tokens;
using Kafka.Connect.Providers;
using Kafka.Connect.Tokens;
using Microsoft.Extensions.DependencyInjection;

namespace Kafka.Connect.Connectors;

public class Connector(
    ILogger<Connector> logger,
    IServiceScopeFactory serviceScopeFactory,
    IConfigurationProvider configurationProvider,
    IExecutionContext executionContext,
    ITokenHandler tokenHandler)
    : IConnector
{
    private readonly PauseTokenSource _pauseTokenSource = new();

    public bool IsPaused => _pauseTokenSource.IsPaused;
    public bool IsStopped { get; private set; }

    public async Task Execute(string connector,  CancellationTokenSource cts)
    {
        executionContext.Initialize(connector, this);
        var connectorConfig = configurationProvider.GetConnectorConfig(connector);

        if (connectorConfig.Paused)
        {
            _pauseTokenSource.Pause();
        }

        while (!cts.IsCancellationRequested) 
        {
            tokenHandler.NoOp();
            await _pauseTokenSource.WaitWhilePaused(cts.Token);

            if (cts.IsCancellationRequested)
            {
                break;
            }

            var taskId = 0;
            logger.Debug("Starting tasks.", new { Tasks = connectorConfig.MaxTasks });

            var tasks = (from scope in Enumerable.Range(1, connectorConfig.MaxTasks)
                    .Select(_ => serviceScopeFactory.CreateScope())
                let task = (ITask)(connectorConfig.Plugin.Type switch
                {
                    ConnectorType.Leader => scope.ServiceProvider.GetService<ILeaderTask>(),
                    ConnectorType.Worker => scope.ServiceProvider.GetService<IWorkerTask>(),
                    ConnectorType.Sink => scope.ServiceProvider.GetService<ISinkTask>(),
                    ConnectorType.Source => scope.ServiceProvider.GetService<ISourceTask>(),
                    _ => throw new ArgumentOutOfRangeException(nameof(ConnectorType), "Invalid connector type.")
                })
                select new { Task = task, Scope = scope, Config = connectorConfig }).ToList();

            await Task.WhenAll(tasks.Select(task =>
            {
                taskId++;
                using (ConnectLog.Task(taskId)) 
                {
                    if (task?.Task == null)
                    {
                        logger.Warning("Unable to load and terminating the task.");
                        return Task.CompletedTask;
                    }

                    logger.Debug("Starting task.");
                    var linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cts.Token);
                    _pauseTokenSource.AddLinkedTokenSource(linkedTokenSource);
                    var sinkTask = task.Task.Execute(connector, taskId, linkedTokenSource);
                    return sinkTask.ContinueWith(t =>
                    {
                        if (t.IsFaulted)
                        {
                            logger.Error("Task is faulted, and will be terminated.", t.Exception?.InnerException);
                        }

                        logger.Debug("Task will be stopped.");
                    }, TaskContinuationOptions.None);
                }
            })).ContinueWith(t =>
            {
                if (t.IsFaulted)
                {
                    logger.Error("Connector is faulted, and will be terminated.", t.Exception?.InnerException);
                }
            }, TaskContinuationOptions.None);

            if(cts.IsCancellationRequested || _pauseTokenSource.IsPaused) continue;

            if (await executionContext.Retry(connector)) continue;
            _pauseTokenSource.Pause();
        }
        logger.Debug( "Shutting down the connector.");

        IsStopped = true;
    }

    public void Pause()
    {
        _pauseTokenSource.Pause();
    }

    public void Resume(IDictionary<string, string> payload)
    {
        _pauseTokenSource.Resume();
    }
}
