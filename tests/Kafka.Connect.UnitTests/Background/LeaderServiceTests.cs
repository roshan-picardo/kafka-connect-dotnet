using System;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect;
using Kafka.Connect.Background;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Providers;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;

namespace UnitTests.Kafka.Connect.Background;

public class LeaderServiceTests
{
    private readonly ILogger<Leader> _logger = Substitute.For<ILogger<Leader>>();
    private readonly ILeader _leader = Substitute.For<ILeader>();
    private readonly IExecutionContext _executionContext = Substitute.For<IExecutionContext>();
    private readonly IConfigurationProvider _configurationProvider = Substitute.For<IConfigurationProvider>();
    private LeaderService _leaderService;

    [Fact]
    public async Task ExecuteAsync_WhenIsWorker_ReturnsImmediately()
    {
        // Arrange
        var completionTcs = new TaskCompletionSource<bool>();
        
        _configurationProvider.IsWorker.Returns(true);
        _configurationProvider.GetNodeName().Returns("test-worker-node");
        
        // Hook to detect if service actually runs
        _logger.When(l => l.Debug(Arg.Is<string>(s => s.Contains("Starting background leader"))))
            .Do(_ => completionTcs.TrySetResult(true));
        
        _leaderService = new LeaderService(_leader, _logger, _executionContext, _configurationProvider);

        // Act
        await _leaderService.StartAsync(CancellationToken.None);
        
        // Wait briefly to ensure nothing happens
        var completedTask = await Task.WhenAny(
            completionTcs.Task,
            Task.Delay(100)
        );

        // Assert - service should not have started
        Assert.NotEqual(completionTcs.Task, completedTask);
        _logger.DidNotReceive().Debug("Starting background leader process...");
        _ = _leader.DidNotReceive().Execute(Arg.Any<CancellationTokenSource>());
        _executionContext.DidNotReceive().Shutdown();
    }

    [Fact]
    public async Task ExecuteAsync_StartsTheLeaderService()
    {
        // Arrange
        var executionTcs = new TaskCompletionSource<bool>();
        var shutdownTcs = new TaskCompletionSource<bool>();
        
        _configurationProvider.IsWorker.Returns(false);
        _configurationProvider.GetNodeName().Returns("test-leader-node");
        
        _leader.When(l => l.Execute(Arg.Any<CancellationTokenSource>()))
            .Do(_ => executionTcs.SetResult(true));
        
        _executionContext.When(e => e.Shutdown())
            .Do(_ => shutdownTcs.SetResult(true));
        
        _leaderService = new LeaderService(_leader, _logger, _executionContext, _configurationProvider);

        // Act
        await _leaderService.StartAsync(CancellationToken.None);
        
        // Wait for execution and cleanup
        await Task.WhenAll(
            executionTcs.Task.WaitAsync(TimeSpan.FromSeconds(5)),
            shutdownTcs.Task.WaitAsync(TimeSpan.FromSeconds(5))
        );

        // Assert
        _logger.Received().Debug("Starting background leader process...");
        _ = _leader.Received().Execute(Arg.Any<CancellationTokenSource>());
        _logger.Received().Debug("Stopping background leader process...");
        _executionContext.Received().Shutdown();
    }

    [Fact]
    public async Task ExecuteAsync_LeaderThrowsException()
    {
        // Arrange
        var shutdownTcs = new TaskCompletionSource<bool>();
        
        _configurationProvider.IsWorker.Returns(false);
        _configurationProvider.GetNodeName().Returns("test-leader-node");
        
        _leader.When(l => l.Execute(Arg.Any<CancellationTokenSource>()))
            .Do(_ => throw new Exception("Test exception"));
        
        _executionContext.When(e => e.Shutdown())
            .Do(_ => shutdownTcs.SetResult(true));
        
        _leaderService = new LeaderService(_leader, _logger, _executionContext, _configurationProvider);

        // Act
        await _leaderService.StartAsync(CancellationToken.None);
        
        // Wait for cleanup
        await shutdownTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Assert
        _logger.Received().Debug("Starting background leader process...");
        _logger.Received().Error("Leader service failed to start.", Arg.Any<Exception>());
        _logger.Received().Debug("Stopping background leader process...");
        _executionContext.Received().Shutdown();
    }

    [Fact]
    public async Task ExecuteAsync_LeaderThrowsExceptionWithCancelledToken()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        var shutdownTcs = new TaskCompletionSource<bool>();
        
        _configurationProvider.IsWorker.Returns(false);
        _configurationProvider.GetNodeName().Returns("test-leader-node");
        
        _leader.When(l => l.Execute(Arg.Any<CancellationTokenSource>())).Do(_ =>
        {
            cts.Cancel();
            throw new Exception("Test exception");
        });
        
        _executionContext.When(e => e.Shutdown())
            .Do(_ => shutdownTcs.SetResult(true));
        
        _leaderService = new LeaderService(_leader, _logger, _executionContext, _configurationProvider);

        // Act
        await _leaderService.StartAsync(cts.Token);
        
        // Wait for cleanup
        await shutdownTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Assert
        Assert.True(cts.IsCancellationRequested);
        _logger.Received().Debug("Starting background leader process...");
        _logger.Received().Error("Leader service failed to start.", Arg.Any<Exception>());
        _logger.Received().Debug("Stopping background leader process...");
        _executionContext.Received().Shutdown();
    }

    [Fact]
    public async Task ExecuteAsync_LeaderExecutesSuccessfully()
    {
        // Arrange
        var executionTcs = new TaskCompletionSource<bool>();
        var shutdownTcs = new TaskCompletionSource<bool>();
        
        _configurationProvider.IsWorker.Returns(false);
        _configurationProvider.GetNodeName().Returns("test-leader-node");
        
        var executeCalled = false;
        _leader.When(l => l.Execute(Arg.Any<CancellationTokenSource>())).Do(_ =>
        {
            executeCalled = true;
            executionTcs.SetResult(true);
        });
        
        _executionContext.When(e => e.Shutdown())
            .Do(_ => shutdownTcs.SetResult(true));
        
        _leaderService = new LeaderService(_leader, _logger, _executionContext, _configurationProvider);

        // Act
        await _leaderService.StartAsync(CancellationToken.None);
        
        // Wait for execution and cleanup
        await Task.WhenAll(
            executionTcs.Task.WaitAsync(TimeSpan.FromSeconds(5)),
            shutdownTcs.Task.WaitAsync(TimeSpan.FromSeconds(5))
        );

        // Assert
        Assert.True(executeCalled);
        _logger.Received().Debug("Starting background leader process...");
        _logger.Received().Debug("Stopping background leader process...");
        _executionContext.Received().Shutdown();
    }

    [Fact]
    public async Task ExecuteAsync_CancellationTokenAlreadyCancelled_DoesNotCancelAgain()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        cts.Cancel();
        
        _configurationProvider.IsWorker.Returns(false);
        _configurationProvider.GetNodeName().Returns("test-leader-node");
        
        _leaderService = new LeaderService(_leader, _logger, _executionContext, _configurationProvider);

        // Act
        await _leaderService.StartAsync(cts.Token);

        // Assert
        _ = _leader.DidNotReceive().Execute(Arg.Any<CancellationTokenSource>());
        _executionContext.DidNotReceive().Shutdown();
    }

    [Fact]
    public async Task ExecuteAsync_UsesConnectLogWithNodeName()
    {
        // Arrange
        var nodeName = "test-leader-node-123";
        var executionTcs = new TaskCompletionSource<bool>();
        var shutdownTcs = new TaskCompletionSource<bool>();
        
        _configurationProvider.IsWorker.Returns(false);
        _configurationProvider.GetNodeName().Returns(nodeName);
        
        _leader.When(l => l.Execute(Arg.Any<CancellationTokenSource>()))
            .Do(_ => executionTcs.SetResult(true));
        
        _executionContext.When(e => e.Shutdown())
            .Do(_ => shutdownTcs.SetResult(true));
        
        _leaderService = new LeaderService(_leader, _logger, _executionContext, _configurationProvider);

        // Act
        await _leaderService.StartAsync(CancellationToken.None);
        
        // Wait for execution and cleanup
        await Task.WhenAll(
            executionTcs.Task.WaitAsync(TimeSpan.FromSeconds(5)),
            shutdownTcs.Task.WaitAsync(TimeSpan.FromSeconds(5))
        );

        // Assert
        _configurationProvider.Received(1).GetNodeName();
        _ = _leader.Received().Execute(Arg.Any<CancellationTokenSource>());
    }

    [Fact]
    public async Task ExecuteAsync_EnsuresShutdownCalledInFinally()
    {
        // Arrange
        var shutdownTcs = new TaskCompletionSource<bool>();
        
        _configurationProvider.IsWorker.Returns(false);
        _configurationProvider.GetNodeName().Returns("test-leader-node");
        
        _leader.Execute(Arg.Any<CancellationTokenSource>()).Throws(new InvalidOperationException("Test error"));
        
        _executionContext.When(e => e.Shutdown())
            .Do(_ => shutdownTcs.SetResult(true));
        
        _leaderService = new LeaderService(_leader, _logger, _executionContext, _configurationProvider);

        // Act
        await _leaderService.StartAsync(CancellationToken.None);
        
        // Wait for cleanup
        await shutdownTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Assert
        _executionContext.Received(1).Shutdown();
        _logger.Received().Debug("Stopping background leader process...");
    }

    [Fact]
    public async Task ExecuteAsync_CreatesLinkedCancellationTokenSource()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        var executionTcs = new TaskCompletionSource<bool>();
        var shutdownTcs = new TaskCompletionSource<bool>();
        
        _configurationProvider.IsWorker.Returns(false);
        _configurationProvider.GetNodeName().Returns("test-leader-node");
        
        CancellationTokenSource capturedCts = null;
        _leader.When(l => l.Execute(Arg.Any<CancellationTokenSource>())).Do(callInfo =>
        {
            capturedCts = callInfo.Arg<CancellationTokenSource>();
            executionTcs.SetResult(true);
        });
        
        _executionContext.When(e => e.Shutdown())
            .Do(_ => shutdownTcs.SetResult(true));
        
        _leaderService = new LeaderService(_leader, _logger, _executionContext, _configurationProvider);

        // Act
        await _leaderService.StartAsync(cts.Token);
        
        // Wait for execution and cleanup
        await Task.WhenAll(
            executionTcs.Task.WaitAsync(TimeSpan.FromSeconds(5)),
            shutdownTcs.Task.WaitAsync(TimeSpan.FromSeconds(5))
        );

        // Assert
        Assert.NotNull(capturedCts);
        _ = _leader.Received(1).Execute(Arg.Any<CancellationTokenSource>());
    }
}
