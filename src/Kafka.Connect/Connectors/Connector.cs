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
using Serilog.Context;

namespace Kafka.Connect.Connectors;

public class Connector : IConnector
{
    private readonly ILogger<Connector> _logger;
    private readonly IServiceScopeFactory _serviceScopeFactory;
    private readonly IConnectHandlerProvider _sinkHandlerProvider;
    private readonly IConfigurationProvider _configurationProvider;
    private readonly IExecutionContext _executionContext;
    private readonly ITokenHandler _tokenHandler;
    private readonly PauseTokenSource _pauseTokenSource;

    public Connector(ILogger<Connector> logger, IServiceScopeFactory serviceScopeFactory,
        IConnectHandlerProvider sinkHandlerProvider,
        IConfigurationProvider configurationProvider, IExecutionContext executionContext, ITokenHandler tokenHandler)
    {
        _logger = logger;
        _serviceScopeFactory = serviceScopeFactory;
        _sinkHandlerProvider = sinkHandlerProvider;
        _configurationProvider = configurationProvider;
        _executionContext = executionContext;
        _tokenHandler = tokenHandler;
        _pauseTokenSource = PauseTokenSource.New();
    }
        
    public bool IsPaused => _pauseTokenSource.IsPaused;
    public bool IsStopped { get; private set; }

    public async Task Execute(string connector,  CancellationTokenSource cts)
    {
        _executionContext.Initialize(connector, this);
        var connectorConfig = _configurationProvider.GetConnectorConfig(connector);
        if (connectorConfig.Paused)
        {
            _pauseTokenSource.Pause();
        }

        var sinkHandler = _sinkHandlerProvider.GetSinkHandler(connectorConfig.Name);

        if (sinkHandler != null)
        {
            await sinkHandler.Startup(connectorConfig.Name);
        }

        while (!cts.IsCancellationRequested) 
        {
            _tokenHandler.DoNothing();
            await _pauseTokenSource.Token.WaitWhilePausedAsync(cts.Token);

            if (cts.IsCancellationRequested)
            {
                break;
            }

            var taskId = 0;
            _logger.Debug("Starting tasks.", new { Tasks = connectorConfig.MaxTasks });

            var tasks = (from scope in Enumerable.Range(1, connectorConfig.MaxTasks)
                    .Select(_ => _serviceScopeFactory.CreateScope())
                let task = connectorConfig.Type == ConnectorType.Sink
                    ? scope.ServiceProvider.GetService<ISinkTask>()
                    : (ITask)scope.ServiceProvider.GetService<ISourceTask>()
                select new { Task = task, Scope = scope, Config = connectorConfig }).ToList();

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
            }, TaskContinuationOptions.None);

            if(cts.IsCancellationRequested || _pauseTokenSource.IsPaused) continue;

            if (await _executionContext.Retry(connector)) continue;
            _pauseTokenSource.Pause();
        }
        _logger.Debug( "Shutting down the connector.");

        if (sinkHandler != null)
        {
            await sinkHandler.Cleanup(connectorConfig.Name);
        }

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