using System;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Tokens;
using Kafka.Connect.Providers;
using Microsoft.Extensions.Hosting;

namespace Kafka.Connect.Background;

public class HealthCheckService(
    ILogger<HealthCheckService> logger,
    IConfigurationProvider configurationProvider,
    IExecutionContext executionContext,
    ITokenHandler tokenHandler)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = configurationProvider.GetHealthCheckConfig();
        await Task.Delay(config.Timeout, stoppingToken);
        logger.Health(executionContext.GetSimpleStatus());
        try
        {
            if (!config.Disabled)
            {
                logger.Debug("Starting the health check service...");
                while (!stoppingToken.IsCancellationRequested)
                {
                    logger.Health(executionContext.GetStatus());
                    await Task.Delay(config.Interval, stoppingToken);
                    tokenHandler.NoOp();
                }
            }
        }
        catch (Exception ex)
        {
            if (ex is TaskCanceledException or OperationCanceledException)
            {
                logger.Trace("Task has been cancelled. Health check service will be terminated.", ex);
            }
            else
            {
                logger.Error(
                    "Health check service reported errors / hasn't started. Please use '/workers/status' Rest API to get the worker status.",
                    ex);
            }
        }
        finally
        {
            logger.Debug(config.Disabled
                ? "Health check service is disabled..."
                : "Stopping the health check service...");
        }
    }
}