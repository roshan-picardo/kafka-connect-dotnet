using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Tokens;
using Kafka.Connect.Providers;
using Kafka.Connect.Tokens;
using Microsoft.Extensions.DependencyInjection;
using Serilog.Context;

[assembly:InternalsVisibleTo("Kafka.Connect.UnitTests")]
namespace Kafka.Connect.Connectors
{
    public class Connector : IConnector
    {
        private readonly ILogger<Connector> _logger;
        private readonly IServiceScopeFactory _serviceScopeFactory;
        private readonly ISinkHandlerProvider _sinkHandlerProvider;
        private readonly IConfigurationProvider _configurationProvider;
        private readonly IExecutionContext _executionContext;
        private readonly ITokenHandler _tokenHandler;
        private PauseTokenSource _pauseTokenSource;

        public Connector(ILogger<Connector> logger, IServiceScopeFactory serviceScopeFactory,
            ISinkHandlerProvider sinkHandlerProvider,
            IConfigurationProvider configurationProvider, IExecutionContext executionContext, ITokenHandler tokenHandler)
        {
            _logger = logger;
            _serviceScopeFactory = serviceScopeFactory;
            _sinkHandlerProvider = sinkHandlerProvider;
            _configurationProvider = configurationProvider;
            _executionContext = executionContext;
            _tokenHandler = tokenHandler;
        }

        public async Task Execute(string connector,  CancellationTokenSource cts)
        {
            var restartsConfig = _configurationProvider.GetRestartsConfig();
            var retryAttempts = restartsConfig.Attempts;
            _executionContext.Add(connector);
            _pauseTokenSource = PauseTokenSource.New();
            //var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var connectorConfig = _configurationProvider.GetConnectorConfig(connector);

            _pauseTokenSource.Toggle(connectorConfig.Paused);

            var sinkHandler = _sinkHandlerProvider.GetSinkHandler(connectorConfig.Name);
            var stopwatch = Stopwatch.StartNew();
            var restarts = 0;

            if (sinkHandler != null)
            {
                await sinkHandler.Startup(connectorConfig.Name);
            }

            while (!cts.IsCancellationRequested) 
            {
                _executionContext.Pause(connectorConfig.Name);
                await _pauseTokenSource.Token.WaitWhilePausedAsync(cts.Token);

                if (cts.IsCancellationRequested)
                {
                   
                    break;
                }
                
                _executionContext.Clear(connectorConfig.Name);
                var taskId = 0;
                _logger.Debug("Starting tasks.", new { Tasks = connectorConfig.MaxTasks });

                _executionContext.Start(connectorConfig.Name);

                var tasks = (from scope in Enumerable.Range(1, connectorConfig.MaxTasks)
                        .Select(_ => _serviceScopeFactory.CreateScope())
                    let task = scope.ServiceProvider.GetService<ISinkTask>()
                    select new {Task = task, Scope = scope, Config = connectorConfig}).ToList();

                await Task.WhenAll(tasks.Select(task =>
                {
                    taskId++;
                    using (LogContext.PushProperty("Task", taskId)) 
                    {
                        if (task?.Task == null)
                        {
                            _logger.Warning("Unable to load and terminating the task.");
                            return Task.CompletedTask;
                        }

                        _logger.Debug("Starting task.", new { Id = $"#{taskId:00}" });
                        var linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cts.Token);
                        _pauseTokenSource.AddLinkedTokenSource(linkedTokenSource);
                        var sinkTask = task.Task.Execute(connector, taskId, linkedTokenSource);
                        return sinkTask.ContinueWith(t =>
                        {
                            if (t.IsFaulted)
                            {
                                _logger.Error("Task is faulted, and will be terminated.", t.Exception?.InnerException);
                            }

                            _logger.Debug("Task will be stopped.");
                        }, TaskContinuationOptions.None);
                    }
                })).ContinueWith(t =>
                {
                    if (t.IsFaulted)
                    {
                        _logger.Error("Connector is faulted, and will be terminated.", t.Exception?.InnerException);
                    }
                }, CancellationToken.None);

                _executionContext.Stop(connectorConfig.Name);

                if(cts.IsCancellationRequested || _pauseTokenSource.IsPaused) continue;

                if (!restartsConfig.EnabledFor.HasFlag(RestartsLevel.Connector)) break;
                
                if (retryAttempts < 0)
                {
                    _logger.Debug("Attempting to restart the Connector.", new { Attempt = ++restarts });
                    continue;
                }

                if (stopwatch.ElapsedMilliseconds >= 30000)
                {
                    retryAttempts = restartsConfig.Attempts;
                }

                if (retryAttempts > 0)
                {
                    await Task.Delay(restartsConfig.PeriodicDelayMs, CancellationToken.None);
                    _logger.Debug("Attempting to restart the Connector.", new{ Attempt = ++restarts});
                    --retryAttempts;
                    stopwatch.Restart();
                    continue;
                }
                _logger.Info( "Restart attempts exhausted the threshold for the Connector.", new { Use = $"/connectors/{connectorConfig.Name}/resume"});
                _tokenHandler.DoNothing();
                await Pause();
            }
            
            _logger.Debug( "Shutting down the connector.");

            if (sinkHandler != null)
            {
                await sinkHandler.Cleanup(connectorConfig.Name);
            }
        }

        public Task Pause()
        {
            _pauseTokenSource.Pause();
            return Task.CompletedTask;
        }

        public Task Resume(IDictionary<string, string> payload)
        {
            _pauseTokenSource.Resume();
            return Task.CompletedTask;
        }

        public async Task Restart(int? delay, IDictionary<string, string> payload)
        {
            await Pause();
            await Task.Delay(delay ?? 1000);
            await Resume(payload);
        }

        
    }
}