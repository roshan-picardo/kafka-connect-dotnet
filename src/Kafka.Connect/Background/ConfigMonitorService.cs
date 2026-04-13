using System;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Providers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Kafka.Connect.Background;

public class ConfigMonitorService(
    ILogger<ConfigMonitorService> logger,
    IServiceScopeFactory serviceScopeFactory,
    IExecutionContext executionContext,
    IConfigurationProvider configurationProvider)
    : BackgroundService
{
    private const string ConfigMonitorConnectorName = "__config-monitor__";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Only run if this is a worker node and leader config is available
        if (!configurationProvider.IsWorker)
        {
            logger.Debug("Config monitor service is disabled (not a worker node).");
            return;
        }

        LeaderConfig leaderConfig;
        try
        {
            leaderConfig = configurationProvider.GetLeaderConfig();
            if (leaderConfig == null || string.IsNullOrEmpty(leaderConfig.Settings))
            {
                logger.Debug("Config monitor service is disabled (no leader configuration found).");
                return;
            }
        }
        catch (Exception ex)
        {
            logger.Debug($"Config monitor service is disabled (leader configuration not available): {ex.Message}");
            return;
        }

        using (ConnectLog.Worker($"{configurationProvider.GetNodeName()}-config-monitor"))
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            try
            {
                logger.Debug("Starting configuration monitor service...");
                logger.Debug($"Monitoring configuration topic for changes. Settings directory: {leaderConfig.Settings}");

                using var scope = serviceScopeFactory.CreateScope();
                var workerTask = scope.ServiceProvider.GetService<IWorkerTask>();
                
                if (workerTask == null)
                {
                    logger.Error("Failed to resolve IWorkerTask for configuration monitoring.");
                    return;
                }

                // Execute the WorkerTask which will monitor the config topic
                await workerTask.Execute(ConfigMonitorConnectorName, 0, cts);
            }
            catch (Exception ex)
            {
                if (ex is TaskCanceledException or OperationCanceledException)
                {
                    logger.Trace("Configuration monitor service has been cancelled.");
                }
                else
                {
                    logger.Error("Configuration monitor service failed.", ex);
                }
                
                if (!cts.IsCancellationRequested)
                {
                    await cts.CancelAsync();
                }
            }
            finally
            {
                logger.Debug("Stopping configuration monitor service...");
                executionContext.Shutdown();
            }
        }
    }
}
