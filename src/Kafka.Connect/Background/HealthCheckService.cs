using System;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin.Tokens;
using Kafka.Connect.Providers;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kafka.Connect.Background
{
    public class HealthCheckService : BackgroundService
    {
        private readonly ILogger<HealthCheckService> _logger;
        private readonly IConfigurationProvider _configurationProvider;
        private readonly IExecutionContext _executionContext;
        private readonly ITokenHandler _tokenHandler;

        public HealthCheckService(ILogger<HealthCheckService> logger, IConfigurationProvider configurationProvider ,
            IExecutionContext executionContext, ITokenHandler tokenHandler)
        {
            _logger = logger;
            _configurationProvider = configurationProvider;
            _executionContext = executionContext;
            _tokenHandler = tokenHandler;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("{@Log}",  new {Log = _executionContext.GetFullDetails()});
            var config = _configurationProvider.GetHealthCheckConfig();
            try
            {
                if (!config.Disabled)
                {
                    _logger.LogDebug("{@Log}", new {Message = "Starting the health check service..."});
                    await Task.Delay(config.InitialDelayMs, stoppingToken);
                    while (!stoppingToken.IsCancellationRequested)
                    {
                        _logger.LogInformation("{@Health}", new {Worker = _executionContext.GetStatus()});
                        await Task.Delay(config.PeriodicDelayMs, stoppingToken);
                        _tokenHandler.DoNothing();
                    }
                }
            }
            catch (Exception ex)
            {
                if (ex is TaskCanceledException or OperationCanceledException)
                {
                    _logger.LogTrace(ex,"{@Log}",
                        new {Message = "Task has been cancelled. Health check service will be terminated."});
                }
                else
                {
                    _logger.LogError(ex, "{@Log}",
                        new
                        {
                            Message =
                                "Health check service reported errors / hasn't started. Please use '/workers/status' Rest API to get the worker status."
                        });
                }
            }
            finally
            {
                _logger.LogDebug("{@Log}",
                    config.Disabled
                        ? new {Message = "Health check service is disabled..."}
                        : new {Message = "Stopping the health check service..."});
            }
        }
    }
}