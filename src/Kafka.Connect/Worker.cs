using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Tokens;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog.Context;

namespace Kafka.Connect
{
    public class Worker : IWorker
    {
        private readonly ILogger<Worker> _logger;
        private readonly IServiceScopeFactory _serviceScopeFactory;
        private readonly IExecutionContext _executionContext;
        private readonly IConfigurationProvider _configurationProvider;
        private PauseTokenSource _pauseTokenSource;
        private readonly IList<(string Name, IConnector Connector)> _connectors;

        private int _retryAttempts = -1;

        public Worker(ILogger<Worker> logger, IServiceScopeFactory serviceScopeFactory,
            IExecutionContext executionContext, IConfigurationProvider configurationProvider)
        {
            _logger = logger;
            _serviceScopeFactory = serviceScopeFactory;
            _executionContext = executionContext;
            _configurationProvider = configurationProvider;
            _connectors = new List<(string Name, IConnector Connector)>();
        }

        public async Task Execute(CancellationToken cancellationToken)
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken); // from here-on use this token to track
            var restartsConfig = _configurationProvider.GetRestartsConfig();
            
            _logger.LogDebug("{@Log}", new {Message = "Starting the worker."});
            //_logger.Timed("Initializing and verifying the configurations.")
              //  .Execute(() => _workerConfig.MergeAndVerify(_logger));
            
            _executionContext.Name(_configurationProvider.GetWorkerName()); // or you could put this in exec context
            var stopwatch = Stopwatch.StartNew();
            var restarts = 0;

            _retryAttempts = restartsConfig.Attempts;

            _pauseTokenSource = PauseTokenSource.New();
            while (!cts.IsCancellationRequested)
            {
                _executionContext.Pause();
                await _pauseTokenSource.Token.WaitWhilePausedAsync(cts.Token);
                if (cts.IsCancellationRequested)
                {
                    break;
                }

                _executionContext.Clear();
                _connectors.Clear();
                try
                {
                    _logger.LogDebug("{@Log}", new { Message = "Starting connectors.", _configurationProvider.GetConnectorConfigs().Count});
                    var connectors = from job in _configurationProvider.GetConnectorConfigs().Select(s =>
                            new {Name = s.Name, Scope = _serviceScopeFactory.CreateScope()})
                        let connector = job.Scope.ServiceProvider.GetService<IConnector>()
                        select new {Connector = connector, job.Name};
                    _executionContext.Start();
                    await Task.WhenAll(connectors.Select(c =>
                        {
                            using (LogContext.PushProperty("Connector", c.Name))
                            {
                                _logger.LogDebug("{@Log}", new {Message = "Connector Starting."});
                                //TODO: _pauseTokenSource.AddSubTaskTokens(c.Cancel); 
                                var connectorTask = c.Connector.Execute(c.Name, PauseTokenSource.New(), cts.Token).ContinueWith(
                                    t =>
                                    {
                                        if (t.IsFaulted && !t.IsCanceled)
                                        {
                                            _logger.LogError(t.Exception?.InnerException, "{@Log}",
                                                new {Message = "Connector is faulted, and is terminated."});
                                        }

                                        _logger.LogDebug("{@Log}", new {Message = "Connector Stopped."});
                                    }, CancellationToken.None);
                                _connectors.Add((c.Name, c.Connector));
                                return connectorTask;
                            }
                        }))
                        .ContinueWith(t =>
                        {
                            if (t.IsFaulted && !t.IsCanceled)
                            {
                                _logger.LogError(t.Exception?.InnerException, "{@Log}", new {Message = "Worker is faulted, and is terminated."});
                            }

                            _executionContext.Stop();
                        }, CancellationToken.None);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "{@Log}", new {Message = "Worker is faulted, and is terminated."});
                }

                if (cts.IsCancellationRequested || _pauseTokenSource.IsPaused ||
                    !restartsConfig.Enabled.HasFlag(RestartsLevel.Worker)) continue;
                
                if (_retryAttempts < 0)
                {
                    await Task.Delay(restartsConfig.PeriodicDelayMs, CancellationToken.None);
                    _logger.LogDebug("{@Log}", new {Message = "Attempting to restart the Worker.", Attempt = ++restarts});
                    continue;
                }

                if (stopwatch.ElapsedMilliseconds >= restartsConfig.RetryWaitTimeMs)
                {
                    _retryAttempts = restartsConfig.Attempts;
                }

                if (_retryAttempts > 0)
                {
                    await Task.Delay(restartsConfig.PeriodicDelayMs, CancellationToken.None);
                    _logger.LogDebug("{@Log}", new {Message = "Attempting to restart the Worker.", Attempt = ++restarts});
                    --_retryAttempts;
                    stopwatch.Restart();
                    continue;
                }

                _logger.LogInformation("{@Log}",
                    new
                    {
                        Message = "Restart attempts exhausted the threshold for the Worker.",
                        Use = "/workers/resume"
                    });
                    
                if (restartsConfig.StopOnFailure)
                {
                    cts.Cancel();
                }
                else
                {
                    _pauseTokenSource.Pause();
                }
            }

            _logger.LogDebug("{@Log}", new {Message = "Shutting down the worker."});
        }

        public Task PauseAsync()
        {
            _pauseTokenSource.Resume();
            return Task.CompletedTask;
        }

        public Task ResumeAsync()
        {
            _pauseTokenSource.Resume();
            return Task.CompletedTask;
        }

        public async Task RestartAsync(int? delayMs)
        {
            await PauseAsync();
            await Task.Delay(delayMs ?? 1000);
            await ResumeAsync();
        }

        public IConnector GetConnector(string name) => _connectors.SingleOrDefault(c=>c.Name == name).Connector;
    }
}