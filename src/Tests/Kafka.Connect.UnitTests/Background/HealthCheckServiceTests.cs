using System;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Background;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Kafka.Connect.Models;
using Kafka.Connect.Plugin.Tokens;
using Kafka.Connect.Providers;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit;

namespace Kafka.Connect.UnitTests.Background
{
    public class HealthCheckServiceTests
    {
        private readonly ILogger<HealthCheckService> _logger;
        private readonly IConfigurationProvider _configurationProvider;
        private readonly IExecutionContext _executionContext;
        private readonly ITokenHandler _tokenHandler;
        private HealthCheckService _healthCheckService;

        public HealthCheckServiceTests()
        {
            _logger = Substitute.For<MockLogger<HealthCheckService>>();
            _configurationProvider = Substitute.For<IConfigurationProvider>();
            _executionContext = Substitute.For<IExecutionContext>();
            _tokenHandler = Substitute.For<ITokenHandler>();
        }

        [Fact]
        public void ExecuteAsync_ServiceNotEnabled()
        {
            _configurationProvider.GetHealthCheckConfig().Returns(new HealthCheckConfig{Disabled = true});
            _healthCheckService =
                new HealthCheckService(_logger, _configurationProvider, _executionContext, _tokenHandler);

            _healthCheckService.StartAsync(GetCancellationToken(1));
            
            _logger.DidNotReceive().Log(LogLevel.Debug, "{@Log}", new {Message = "Starting the health check service..."});
            _logger.Received().Log(LogLevel.Debug, "{@Log}", new {Message = "Health check service is disabled..."});
        }
        
        [Fact]
        public void ExecuteAsync_ServiceEnabledWithHealthCheckLogs()
        {
            _configurationProvider.GetHealthCheckConfig().Returns(new HealthCheckConfig{InitialDelayMs = 1, PeriodicDelayMs = 1});
            var log = new WorkerContext { Name = "My Worker"};
            _executionContext.GetStatus().Returns(log);
            _healthCheckService =
                new HealthCheckService(_logger, _configurationProvider, _executionContext, _tokenHandler);
            
            _healthCheckService.StartAsync(GetCancellationToken(1));
            
            while (!_healthCheckService.ExecuteTask.IsCompletedSuccessfully)
            {
                // wait for the task to complete
            }
            
            _logger.Received().Log(LogLevel.Debug, "{@Log}", new {Message = "Starting the health check service..."});
            _logger.Received().Log(LogLevel.Information, "{@Health}", new {Worker = log});
            _logger.Received().Log(LogLevel.Debug, "{@Log}", new {Message = "Stopping the health check service..."});
        }
        
        [Theory]
        [InlineData(typeof(Exception), LogLevel.Error, "Health check service reported errors / hasn't started. Please use '/workers/status' Rest API to get the worker status.")]
        [InlineData(typeof(TaskCanceledException), LogLevel.Trace, "Task has been cancelled. Health check service will be terminated.")]
        [InlineData(typeof(OperationCanceledException), LogLevel.Trace, "Task has been cancelled. Health check service will be terminated.")]
        public void ExecuteAsync_ServiceEnabledThrowsException(Type exType, LogLevel expectedLevel, string expectedMessage)
        {
            _configurationProvider.GetHealthCheckConfig().Returns(new HealthCheckConfig{InitialDelayMs = 1, PeriodicDelayMs = 1});
            _healthCheckService =
                new HealthCheckService(_logger, _configurationProvider, _executionContext, _tokenHandler);
            var exception = Activator.CreateInstance(exType, "Unit Tests Exception.") as Exception ?? new Exception();
            _executionContext.When(e => e.GetStatus()).Do(_ => throw exception );
            _healthCheckService.StartAsync(GetCancellationToken(1));
            
            while (!_healthCheckService.ExecuteTask.IsCompletedSuccessfully)
            {
                // wait for the task to complete
            }
            
            _logger.Received().Log(LogLevel.Debug, "{@Log}", new {Message = "Starting the health check service..."});
            _logger.Received().Log(expectedLevel, Arg.Any<Exception>(), "{@Log}", new {Message = expectedMessage});
            _logger.Received().Log(LogLevel.Debug, "{@Log}", new {Message = "Stopping the health check service..."});
        }
        
        private CancellationToken GetCancellationToken(int loop)
        {
            var cts = new CancellationTokenSource();

            _tokenHandler.When(k => k.DoNothing()).Do(_ =>
            {
                if (--loop == 0) cts.Cancel();
            });
            return cts.Token;
        }
    }
}