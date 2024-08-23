using System;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Tokens;
using Kafka.Connect.Providers;
using Microsoft.Extensions.Hosting;

namespace Kafka.Connect.Background
{
    public class HealthCheckService : BackgroundService
    {
        private readonly ILogger<HealthCheckService> _logger;
        private readonly IConfigurationProvider _configurationProvider;
        private readonly IExecutionContext _executionContext;
        private readonly ITokenHandler _tokenHandler;

        public HealthCheckService(ILogger<HealthCheckService> logger, IConfigurationProvider configurationProvider,
            IExecutionContext executionContext, ITokenHandler tokenHandler)
        {
            _logger = logger;
            _configurationProvider = configurationProvider;
            _executionContext = executionContext;
            _tokenHandler = tokenHandler;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var config = _configurationProvider.GetHealthCheckConfig();
            await Task.Delay(config.InitialDelayMs, stoppingToken);
            // _logger.Health(_executionContext.GetFullDetails() as object);
            try
            {
                if (!config.Disabled)
                {
                    _logger.Debug("Starting the health check service...");
                    while (!stoppingToken.IsCancellationRequested)
                    {
                        _logger.Health(_executionContext.GetStatus());
                        await Task.Delay(config.PeriodicDelayMs, stoppingToken);
                        _tokenHandler.NoOp();
                    }
                }
            }
            catch (Exception ex)
            {
                if (ex is TaskCanceledException or OperationCanceledException)
                {
                    _logger.Trace("Task has been cancelled. Health check service will be terminated.", ex);
                }
                else
                {
                    _logger.Error(
                        "Health check service reported errors / hasn't started. Please use '/workers/status' Rest API to get the worker status.",
                        ex);
                }
            }
            finally
            {
                _logger.Debug(config.Disabled
                    ? "Health check service is disabled..."
                    : "Stopping the health check service...");
            }
        }
    }
}