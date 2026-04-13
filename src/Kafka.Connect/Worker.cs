using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Providers;
using Kafka.Connect.Tokens;
using Microsoft.Extensions.DependencyInjection;
//H2KGH3AL1059

namespace Kafka.Connect;

public class Worker(
    ILogger<Worker> logger,
    IServiceScopeFactory serviceScopeFactory,
    IExecutionContext executionContext,
    IConfigurationProvider configurationProvider)
    : IWorker
{
    private PauseTokenSource _pauseTokenSource;
    private readonly ConcurrentDictionary<string, (Task Task, CancellationTokenSource Cts)> _tasks = new();
    private CancellationTokenSource _waitCancellation = new();

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
            await _pauseTokenSource.WaitWhilePaused(cts.Token);
            if (cts.IsCancellationRequested)
            {
                break;
            }
            try
            {
                await SyncConnectors(cts.Token);
                do
                {
                    var currentTasks = _tasks.Values.Select(v => v.Task).ToArray();
                    
                    if (currentTasks.Length == 0)
                    {
                        await Task.Delay(100, cts.Token);
                        continue;
                    }
                    
                    try
                    {
                        await Task.WhenAll(currentTasks).WaitAsync(_waitCancellation.Token);
                    }
                    catch (OperationCanceledException)
                    {
                        _waitCancellation = new CancellationTokenSource();
                    }
                    catch (Exception ex)
                    {
                        logger.Error("Worker is faulted, and is terminated.", ex);
                    }
                } while (_tasks.Values.Any(v => !v.Task.IsCompleted));
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

    private async Task SyncConnectors(CancellationToken token)
    {
        logger.Debug("Syncing connectors with current configuration.");
        
        configurationProvider.ReloadWorkerConfig();
        
        var configuredConnectors = configurationProvider.GetAllConnectorConfigs()
            .Select(c => c.Name)
            .ToHashSet();
        
        var runningConnectors = _tasks.Keys.ToHashSet();

        foreach (var connector in configuredConnectors.Except(runningConnectors))
        {
            AddConnectorTask(connector, token);
        }

        foreach (var connector in runningConnectors.Except(configuredConnectors))
        {
            await Remove(connector);
        }

        foreach (var connector in runningConnectors.Intersect(configuredConnectors))
        {
            logger.Debug($"Connector '{connector}' configuration may have changed. Restarting.");
            await Remove(connector);
            await Task.Delay(500, token); // Brief delay before restart
            AddConnectorTask(connector, token);
        }
        
        logger.Debug($"Connector sync complete. Running connectors: {_tasks.Count}");
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

            var linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(token);
            _pauseTokenSource.AddLinkedTokenSource(linkedTokenSource);
            
            var connectorTask = connector.Execute(name, linkedTokenSource).ContinueWith(
                t =>
                {
                    if (t is { IsFaulted: true, IsCanceled: false })
                    {
                        logger.Error("Connector is faulted, and is terminated.", t.Exception?.InnerException);
                    }
                    _tasks.TryRemove(name, out _);
                    logger.Info($"Stopped connector: {name}");
                }, CancellationToken.None);
            
            _tasks.TryAdd(name, (connectorTask, linkedTokenSource));
        }
    }

    public Task Add(string connector)
    {
        if (_tasks.ContainsKey(connector))
        {
            logger.Warning($"Connector '{connector}' is already running.");
            return Task.CompletedTask;
        }
        
        AddConnectorTask(connector, CancellationToken.None);
        _waitCancellation.Cancel();
        return Task.CompletedTask;
    }

    public Task Remove(string connector)
    {
        if (_tasks.TryRemove(connector, out var taskInfo))
        {
            logger.Debug($"Stopping connector '{connector}'.");
            taskInfo.Cts.Cancel();
            _waitCancellation.Cancel();
        }
        else
        {
            logger.Warning($"Connector '{connector}' not found.");
        }
        
        return Task.CompletedTask;
    }

    public async Task Refresh(string connector, bool isDelete = false)
    {
        // Reload worker configuration to get latest settings
        configurationProvider.ReloadWorkerConfig();
        
        // Check if connector is currently running
        var connectorExists = _tasks.ContainsKey(connector);
        
        if (isDelete)
        {
            // Delete operation - remove connector if it exists
            if (connectorExists)
            {
                logger.Debug($"Deleting connector '{connector}'.");
                await Remove(connector);
            }
            else
            {
                logger.Debug($"Connector '{connector}' not running, nothing to delete.");
            }
        }
        else
        {
            // Add or update operation
            if (connectorExists)
            {
                // Connector already running - restart it with new configuration
                logger.Debug($"Restarting connector '{connector}' with updated configuration.");
                await Remove(connector);
                await Task.Delay(500); // Brief delay before restart
                await Add(connector);
            }
            else
            {
                // New connector - add it
                logger.Debug($"Starting new connector '{connector}'.");
                await Add(connector);
            }
        }
    }

    public bool IsRunning(string connector) => _tasks.ContainsKey(connector);

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
