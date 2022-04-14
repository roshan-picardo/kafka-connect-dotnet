using System;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Background;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;

namespace Kafka.Connect.Tests.Background
{
    public class WorkerServiceTests
    {
        private readonly ILogger<WorkerService> _logger;
        private readonly IWorker _worker;
        private WorkerService _workerService;

        public WorkerServiceTests()
        {
            _logger = Substitute.For<MockLogger<WorkerService>>();
            _worker = Substitute.For<IWorker>();

            _workerService = new WorkerService(_logger, _worker);
        }

        [Fact]
        public void ExecuteAsync_StartsTheWorkerService()
        {
            _workerService.StartAsync(CancellationToken.None);
            
            _logger.Received().Log(LogLevel.Debug, "{@Log}", new {Message = "Starting background worker process..."});
            _logger.Received().Log(LogLevel.Debug, "{@Log}", new {Message = "Stopping background worker process..."});
        }
        
        [Fact]
        public async Task ExecuteAsync_WorkerThrowsException()
        {
            var cts = new CancellationTokenSource();
            _worker.Execute(Arg.Any<CancellationToken>()).Throws<Exception>();
            
            await _workerService.StartAsync(cts.Token);
            
            //Assert.True(cts.Token.IsCancellationRequested);
            _logger.Received().Log(LogLevel.Debug, "{@Log}", new {Message = "Starting background worker process..."});
            _logger.Received().Log(LogLevel.Error, Arg.Any<Exception>(),  "{@Log}", new {Message = "Worker service failed to start."});
            _logger.Received().Log(LogLevel.Debug, "{@Log}", new {Message = "Stopping background worker process..."});
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
            _logger.Received().Log(LogLevel.Debug, "{@Log}", new {Message = "Starting background worker process..."});
            _logger.Received().Log(LogLevel.Error, Arg.Any<Exception>(),  "{@Log}", new {Message = "Worker service failed to start."});
            _logger.Received().Log(LogLevel.Debug, "{@Log}", new {Message = "Stopping background worker process..."});
        }

    }
}