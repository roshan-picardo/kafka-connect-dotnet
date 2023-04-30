using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Providers;
using Kafka.Connect.Tokens;
using Microsoft.Extensions.DependencyInjection;
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

        public async Task Execute(CancellationTokenSource cts)
        {
            _configurationProvider.Validate();
            //var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken); // from here-on use this token to track
            var restartsConfig = _configurationProvider.GetRestartsConfig();

            _logger.Debug("Starting the worker.");
            
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
                    _logger.Debug("Starting connectors.", new { _configurationProvider.GetAllConnectorConfigs().Count});
                    var connectors = from job in _configurationProvider.GetAllConnectorConfigs().Select(s =>
                            new {Name = s.Name, Scope = _serviceScopeFactory.CreateScope()})
                        let connector = job.Scope.ServiceProvider.GetService<IConnector>()
                        select new {Connector = connector, job.Name};
                    _executionContext.Start();
                    await Task.WhenAll(connectors.Select(connector =>
                        {
                            using (LogContext.PushProperty("Connector", connector.Name))
                            {
                                if (connector?.Connector == null)
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
                                _connectors.Add((connector.Name, connector.Connector));
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
                    _executionContext.Stop();
                }
                catch (Exception ex)
                {
                    _logger.Error("Worker is faulted, and is terminated.", ex);
                }

                if (cts.IsCancellationRequested || _pauseTokenSource.IsPaused ||
                    !restartsConfig.EnabledFor.HasFlag(RestartsLevel.Worker)) continue;
                
                if (_retryAttempts < 0)
                {
                    await Task.Delay(restartsConfig.PeriodicDelayMs, CancellationToken.None);
                    _logger.Debug("Attempting to restart the Worker.", new { Attempt = ++restarts });
                    continue;
                }

                if (stopwatch.ElapsedMilliseconds >= restartsConfig.RetryWaitTimeMs)
                {
                    _retryAttempts = restartsConfig.Attempts;
                }

                if (_retryAttempts > 0)
                {
                    await Task.Delay(restartsConfig.PeriodicDelayMs, CancellationToken.None);
                    _logger.Debug("Attempting to restart the Worker.", new { Attempt = ++restarts });
                    --_retryAttempts;
                    stopwatch.Restart();
                    continue;
                }

                _logger.Info("Restart attempts exhausted the threshold for the Worker.", new { Use = "/workers/resume" });
                    
                if (restartsConfig.StopOnFailure)
                {
                    cts.Cancel();
                }
                else
                {
                    _pauseTokenSource.Pause();
                }
            }

            _logger.Debug("Shutting down the worker.");
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