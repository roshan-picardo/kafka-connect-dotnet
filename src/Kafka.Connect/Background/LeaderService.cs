using System;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Providers;
using Microsoft.Extensions.Hosting;

namespace Kafka.Connect.Background;

public class LeaderService(
    ILeader leader,
    ILogger<Leader> logger,
    IExecutionContext executionContext,
    IConfigurationProvider configurationProvider) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (configurationProvider.IsWorker) return;
        var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        try
        {
            logger.Debug("Starting background leader process...");

            await leader.Execute(cts);
        }
        catch (Exception ex)
        {
            logger.Error("Leader service failed to start.", ex);
            if (!cts.IsCancellationRequested)
            {
                await cts.CancelAsync();
            }
        }
        finally
        {
            logger.Debug("Stopping background leader process...");
            executionContext.Shutdown();
        }
    }
}