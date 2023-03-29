using System;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Connectors;
using Kafka.Connect.Logging;
using Microsoft.Extensions.Hosting;

namespace Kafka.Connect.Background
{
    public class WorkerService : BackgroundService
    {
        private readonly ILogger<WorkerService> _logger;
        private readonly IWorker _worker;
        private readonly IExecutionContext _executionContext;

        public WorkerService(ILogger<WorkerService> logger, IWorker worker, IExecutionContext executionContext)
        {
            _logger = logger;
            _worker = worker;
            _executionContext = executionContext;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            try
            {
                _logger.Debug("Starting background worker process...");

                await _worker.Execute(cts.Token);
            }
            catch (Exception ex)
            {
                _logger.Error("Worker service failed to start.", ex);
                if (!cts.IsCancellationRequested)
                {
                    cts.Cancel();
                }
            }
            finally
            {
                _logger.Debug("Stopping background worker process...");
                _executionContext.Shutdown();
            }
        }
    }
}