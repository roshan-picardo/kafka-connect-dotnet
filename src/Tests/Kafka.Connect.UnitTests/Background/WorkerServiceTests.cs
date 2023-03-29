using System;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Background;
using Kafka.Connect.Connectors;
using Kafka.Connect.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;

namespace Kafka.Connect.UnitTests.Background
{
    public class WorkerServiceTests
    {
        private readonly ILogger<WorkerService> _logger;
        private readonly IWorker _worker;
        private readonly WorkerService _workerService;
        private readonly IExecutionContext _executionContext;

        public WorkerServiceTests()
        {
            _logger = Substitute.For<ILogger<WorkerService>>();
            _worker = Substitute.For<IWorker>();
            _executionContext = Substitute.For<IExecutionContext>();

            _workerService = new WorkerService(_logger, _worker, _executionContext);
        }

        [Fact]
        public void ExecuteAsync_StartsTheWorkerService()
        {
            _workerService.StartAsync(CancellationToken.None);
            
            _logger.Received().Debug("Starting background worker process...");
            _logger.Received().Debug("Stopping background worker process...");
            _executionContext.Received().Shutdown();
        }
        
        [Fact]
        public async Task ExecuteAsync_WorkerThrowsException()
        {
            var cts = new CancellationTokenSource();
            _worker.Execute(Arg.Any<CancellationToken>()).Throws<Exception>();
            
            await _workerService.StartAsync(cts.Token);
            
            _logger.Received().Debug("Starting background worker process...");
            _logger.Received().Error( "Worker service failed to start.", Arg.Any<Exception>());
            _logger.Received().Debug("Stopping background worker process...");
            _executionContext.Received().Shutdown();
        }
        
        [Fact]
        public async Task ExecuteAsync_WorkerThrowsExceptionWithCancelledToken()
        {
            var cts = new CancellationTokenSource();
            _worker.When(w=> w.Execute(Arg.Any<CancellationToken>())).Do(_=>
            {
                cts.Cancel();
                throw new Exception();
            });
            await _workerService.StartAsync(cts.Token);
            
            Assert.True(cts.IsCancellationRequested);
            _logger.Received().Debug( "Starting background worker process...");
            _logger.Received().Error( "Worker service failed to start.", Arg.Any<Exception>());
            _logger.Received().Debug("Stopping background worker process...");
            _executionContext.Received().Shutdown();
        }
    }
}
