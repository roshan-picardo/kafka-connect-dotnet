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

namespace UnitTests.Kafka.Connect.Background
{
    public class HealthCheckServiceTests
    {
        private readonly global::Kafka.Connect.Plugin.Logging.ILogger<HealthCheckService> _logger;
        private readonly IConfigurationProvider _configurationProvider;
        private readonly IExecutionContext _executionContext;
        private readonly ITokenHandler _tokenHandler;
        private HealthCheckService _healthCheckService;

        public HealthCheckServiceTests()
        {
            _logger = Substitute.For<global::Kafka.Connect.Plugin.Logging.ILogger<HealthCheckService>>();
            _configurationProvider = Substitute.For<IConfigurationProvider>();
            _executionContext = Substitute.For<IExecutionContext>();
            _tokenHandler = Substitute.For<ITokenHandler>();
        }

        [Fact]
        public void ExecuteAsync_ServiceNotEnabled()
        {
            _configurationProvider.GetHealthCheckConfig().Returns(new HealthCheckConfig{Disabled = true, InitialDelayMs = 1});
            _healthCheckService =
                new HealthCheckService(_logger, _configurationProvider, _executionContext, _tokenHandler);

            _healthCheckService.StartAsync(GetCancellationToken(1));
            while (!_healthCheckService.ExecuteTask.IsCompletedSuccessfully)
            {
                // wait for the task to complete
            }
            
            _logger.DidNotReceive().Debug("Starting the health check service...");
            _logger.Received().Debug("Health check service is disabled...");
        }
        
        [Fact]
        public void ExecuteAsync_ServiceEnabledWithHealthCheckLogs()
        {
            _configurationProvider.GetHealthCheckConfig().Returns(new HealthCheckConfig{InitialDelayMs = 1, PeriodicDelayMs = 1});
            var log = new WorkerContext();
            _executionContext.GetStatus().Returns(log);
            _healthCheckService =
                new HealthCheckService(_logger, _configurationProvider, _executionContext, _tokenHandler);
            
            _healthCheckService.StartAsync(GetCancellationToken(1));
            
            while (!_healthCheckService.ExecuteTask.IsCompletedSuccessfully)
            {
                // wait for the task to complete
            }
            
            _logger.Received().Debug("Starting the health check service...");
            _logger.Received().Health(log);
            _logger.Received().Debug("Stopping the health check service...");
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

            _logger.Received().Debug("Starting the health check service...");
            switch (expectedLevel)
            {
                case LogLevel.Error:
                    _logger.Received().Error(expectedMessage, Arg.Any<Exception>());
                    break;
                case LogLevel.Trace:
                    _logger.Received().Trace(expectedMessage, Arg.Any<Exception>());
                    break;
            }

            _logger.Received().Debug("Stopping the health check service...");
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