using System;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Providers;
using Microsoft.Extensions.Hosting;

namespace Kafka.Connect.Background;

public class WorkerService(
    ILogger<WorkerService> logger,
    IWorker worker,
    IExecutionContext executionContext,
    IConfigurationProvider configurationProvider)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if(configurationProvider.IsLeader) return;
        using (ConnectLog.Worker(configurationProvider.GetNodeName()))
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            try
            {
                logger.Debug("Starting background worker process...");

                await worker.Execute(cts);
            }
            catch (Exception ex)
            {
                logger.Error("Worker service failed to start.", ex);
                if (!cts.IsCancellationRequested)
                {
                    await cts.CancelAsync();
                }
            }
            finally
            {
                logger.Debug("Stopping background worker process...");
                executionContext.Shutdown();
            }
        }
    }
}