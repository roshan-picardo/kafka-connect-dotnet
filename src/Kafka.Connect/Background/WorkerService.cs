using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kafka.Connect.Background
{
    public class WorkerService : BackgroundService
    {
        private readonly ILogger<WorkerService> _logger;
        private readonly IWorker _worker;

        public WorkerService(ILogger<WorkerService> logger, IWorker worker)
        {
            _logger = logger;
            _worker = worker;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            try
            {
                _logger.LogDebug("{@Log}", new {Message = "Starting background worker process..."});

                await _worker.Execute(cts.Token);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "{@Log}", new {Message = "Worker service failed to start."});
                if (!cts.IsCancellationRequested)
                {
                    cts.Cancel();
                }
            }
            finally
            {
                _logger.LogDebug("{@Log}", new {Message = "Stopping background worker process..."});
            }
        }
    }
}