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

namespace UnitTests.Kafka.Connect.Background;

public class HealthCheckServiceTests
{
    private readonly global::Kafka.Connect.Plugin.Logging.ILogger<HealthCheckService> _logger = Substitute.For<global::Kafka.Connect.Plugin.Logging.ILogger<HealthCheckService>>();
    private readonly IConfigurationProvider _configurationProvider = Substitute.For<IConfigurationProvider>();
    private readonly IExecutionContext _executionContext = Substitute.For<IExecutionContext>();
    private readonly ITokenHandler _tokenHandler = Substitute.For<ITokenHandler>();
    private HealthCheckService _healthCheckService;

    [Fact]
    public async Task ExecuteAsync_ServiceNotEnabled()
    {
        // Arrange
        var completionTcs = new TaskCompletionSource<bool>();
        
        _configurationProvider.GetHealthCheckConfig().Returns(new HealthCheckConfig{Disabled = true, Timeout = 1});
        
        // Hook into the finally block log message
        _logger.When(l => l.Debug("Health check service is disabled..."))
            .Do(_ => completionTcs.SetResult(true));
        
        _healthCheckService = new HealthCheckService(_logger, _configurationProvider, _executionContext, _tokenHandler);

        // Act
        await _healthCheckService.StartAsync(CancellationToken.None);
        
        // Wait for completion
        await completionTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Assert
        _logger.DidNotReceive().Debug("Starting the health check service...");
        _logger.Received().Debug("Health check service is disabled...");
    }
        
    [Fact]
    public async Task ExecuteAsync_ServiceEnabledWithHealthCheckLogs()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        var completionTcs = new TaskCompletionSource<bool>();
        var loopCount = 0;
        
        _configurationProvider.GetHealthCheckConfig().Returns(new HealthCheckConfig{Timeout = 1, Interval = 1});
        var log = new WorkerContext();
        _executionContext.GetStatus().Returns(log);
        
        // Cancel after first iteration
        _logger.When(l => l.Health(Arg.Any<WorkerContext>())).Do(_ =>
        {
            if (++loopCount >= 1)
            {
                cts.Cancel();
            }
        });
        
        // Hook into the finally block
        _logger.When(l => l.Debug("Stopping the health check service..."))
            .Do(_ => completionTcs.SetResult(true));
        
        _healthCheckService = new HealthCheckService(_logger, _configurationProvider, _executionContext, _tokenHandler);

        // Act
        await _healthCheckService.StartAsync(cts.Token);
        
        // Wait for completion
        await completionTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Assert
        _logger.Received().Debug("Starting the health check service...");
        _logger.Received().Health(log);
        _logger.Received().Debug("Stopping the health check service...");
    }
        
    [Theory]
    [InlineData(typeof(Exception), LogLevel.Error, "Health check service reported errors / hasn't started. Please use '/workers/status' Rest API to get the worker status.")]
    [InlineData(typeof(TaskCanceledException), LogLevel.Trace, "Task has been cancelled. Health check service will be terminated.")]
    [InlineData(typeof(OperationCanceledException), LogLevel.Trace, "Task has been cancelled. Health check service will be terminated.")]
    public async Task ExecuteAsync_ServiceEnabledThrowsException(Type exType, LogLevel expectedLevel, string expectedMessage)
    {
        // Arrange
        var completionTcs = new TaskCompletionSource<bool>();
        
        _configurationProvider.GetHealthCheckConfig().Returns(new HealthCheckConfig{Timeout = 1, Interval = 1});
        
        var exception = Activator.CreateInstance(exType, "Unit Tests Exception.") as Exception ?? new Exception();
        _executionContext.When(e => e.GetStatus()).Do(_ => throw exception);
        
        // Hook into the finally block
        _logger.When(l => l.Debug("Stopping the health check service..."))
            .Do(_ => completionTcs.SetResult(true));
        
        _healthCheckService = new HealthCheckService(_logger, _configurationProvider, _executionContext, _tokenHandler);

        // Act
        await _healthCheckService.StartAsync(CancellationToken.None);
        
        // Wait for completion
        await completionTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Assert
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

    [Fact]
    public async Task ExecuteAsync_ServiceRunsMultipleIterations()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        var completionTcs = new TaskCompletionSource<bool>();
        var loopCount = 0;
        var expectedIterations = 3;
        
        _configurationProvider.GetHealthCheckConfig().Returns(new HealthCheckConfig{Timeout = 1, Interval = 1});
        var log = new WorkerContext();
        _executionContext.GetStatus().Returns(log);
        
        // Cancel after N iterations
        _logger.When(l => l.Health(Arg.Any<WorkerContext>())).Do(_ =>
        {
            if (++loopCount >= expectedIterations)
            {
                cts.Cancel();
            }
        });
        
        // Hook into the finally block
        _logger.When(l => l.Debug("Stopping the health check service..."))
            .Do(_ => completionTcs.SetResult(true));
        
        _healthCheckService = new HealthCheckService(_logger, _configurationProvider, _executionContext, _tokenHandler);

        // Act
        await _healthCheckService.StartAsync(cts.Token);
        
        // Wait for completion
        await completionTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Assert
        Assert.Equal(expectedIterations, loopCount);
        _logger.Received().Debug("Starting the health check service...");
        _logger.Received(expectedIterations).Health(log);
        _logger.Received().Debug("Stopping the health check service...");
    }

    [Fact]
    public async Task ExecuteAsync_ServiceCancelsImmediately()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        cts.Cancel(); // Cancel before starting
        
        _configurationProvider.GetHealthCheckConfig().Returns(new HealthCheckConfig{Timeout = 1, Interval = 1});
        
        _healthCheckService = new HealthCheckService(_logger, _configurationProvider, _executionContext, _tokenHandler);

        // Act
        await _healthCheckService.StartAsync(cts.Token);

        // Assert - service should not run when startup token is already cancelled
        _logger.DidNotReceive().Debug("Starting the health check service...");
        _logger.DidNotReceive().Debug("Stopping the health check service...");
    }
}
