using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Providers;
using Kafka.Connect.Tokens;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog.Context;

[assembly:InternalsVisibleTo("Kafka.Connect.Tests")]
namespace Kafka.Connect.Connectors
{
    public class Connector : IConnector
    {
        private readonly ILogger<Connector> _logger;
        private readonly IServiceScopeFactory _serviceScopeFactory;
        private readonly ISinkHandlerProvider _sinkHandlerProvider;
        private readonly IConfigurationProvider _configurationProvider;
        private readonly IExecutionContext _executionContext;
        private PauseTokenSource _pauseTokenSource;

        public Connector(ILogger<Connector> logger, IServiceScopeFactory serviceScopeFactory,
            ISinkHandlerProvider sinkHandlerProvider,
            IConfigurationProvider configurationProvider, IExecutionContext executionContext)
        {
            _logger = logger;
            _serviceScopeFactory = serviceScopeFactory;
            _sinkHandlerProvider = sinkHandlerProvider;
            _configurationProvider = configurationProvider;
            _executionContext = executionContext;
        }

        public async Task Execute(string connector, PauseTokenSource pts, CancellationToken cancellationToken)
        {
            var restartsConfig = _configurationProvider.GetRestartsConfig();
            var retryAttempts = restartsConfig.Attempts;
            _executionContext.Add(connector);
            _pauseTokenSource = pts;
            var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var connectorConfig = _configurationProvider.GetConnectorConfig(connector);

            pts.Toggle(connectorConfig.Paused);

            var sinkHandler = _sinkHandlerProvider.GetSinkHandler(connectorConfig.Plugin, connectorConfig.Sink.Handler);
            var stopwatch = Stopwatch.StartNew();
            var restarts = 0;

            if (sinkHandler != null)
            {
                await _logger.Timed("Running sink handler startup script.").Execute(async () => await sinkHandler.Startup(connectorConfig.Name));
            }

            while (!cts.IsCancellationRequested) 
            {
                _executionContext.Pause(connectorConfig.Name);
                await pts.Token.WaitWhilePausedAsync(cts.Token);

                if (cts.IsCancellationRequested)
                {
                    break;
                }
                
                _executionContext.Clear(connectorConfig.Name);
                var taskId = 1;
                _logger.LogDebug("{@Log}", new {Message = "Starting tasks.", Max = connectorConfig.MaxTasks});

                _executionContext.Start(connectorConfig.Name);

                var tasks = (from scope in Enumerable.Range(1, connectorConfig.MaxTasks)
                        .Select(_ => _serviceScopeFactory.CreateScope())
                    let task = scope.ServiceProvider.GetService<ISinkTask>()
                    select new {Task = task, Scope = scope, Config = connectorConfig}).ToList();

                await Task.WhenAll(tasks.Select(task =>
                {
                    //task.Config.TaskId = taskId++;
                    //pts.AddSubTaskTokens(task.Token);
                    using (LogContext.PushProperty("Task", task.Config.MaxTasks)) 
                    {
                        _logger.LogDebug("{@Log}", new {Message = "Starting task.", Id = "#{t.Config.TaskId:00}"});
                        var sinkTask = task.Task.Execute(connector, taskId++, cts.Token);
                        return sinkTask.ContinueWith(t =>
                        {
                            if (t.IsFaulted)
                            {
                                _logger.LogError(t.Exception?.InnerException, "{@Log}", new { Message = "Task is faulted, and will be terminated."});
                            }

                            _logger.LogDebug("{@Log}", new {Message = "Task will be Stopped."});
                        }, TaskContinuationOptions.None);
                    }
                })).ContinueWith(t =>
                {
                    if (t.IsFaulted)
                    {
                        _logger.LogError(t.Exception?.InnerException, "{@Log}", new { Message = "Connector is faulted, and will be terminated."});
                    }
                    _executionContext.Stop(connectorConfig.Name);
                }, CancellationToken.None);


                if (cts.IsCancellationRequested || _pauseTokenSource.IsPaused ||
                    !restartsConfig.Enabled.HasFlag(RestartsLevel.Connector)) continue;
                if (retryAttempts < 0)
                {
                    _logger.LogDebug("{@Log}", new {Message = "Attempting to restart the Connector.", Attempt = ++restarts});
                    continue;
                }

                if (stopwatch.ElapsedMilliseconds >= 30000)
                {
                    retryAttempts = restartsConfig.Attempts;
                }

                if (retryAttempts > 0)
                {
                    await Task.Delay(restartsConfig.PeriodicDelayMs, CancellationToken.None);
                    _logger.LogDebug("{@Log}", new {Message = "Attempting to restart the Connector.", Attempt = ++restarts});
                    --retryAttempts;
                    stopwatch.Restart();
                    continue;
                }
                _logger.LogInformation("{@Log}", new {Message = "Restart attempts exhausted the threshold for the Connector.", Use = $"/connectors/{connectorConfig.Name}/resume"});
                await Pause();
            }
            _logger.LogDebug("{@Log}", new {Message = "Shutting down the connector."});

            if (sinkHandler != null)
            {
                await _logger.Timed("Running sink handler cleanup script.").Execute(async () => await sinkHandler.Cleanup(connectorConfig.Name));
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