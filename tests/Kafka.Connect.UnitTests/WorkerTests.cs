using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Providers;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect;

public class WorkerTests
{
    private readonly ILogger<Worker> _logger = Substitute.For<ILogger<Worker>>();
    private readonly IServiceScopeFactory _scopeFactory = Substitute.For<IServiceScopeFactory>();
    private readonly IExecutionContext _executionContext = Substitute.For<IExecutionContext>();
    private readonly IConfigurationProvider _configurationProvider = Substitute.For<IConfigurationProvider>();

    [Fact]
    public async Task Execute_WhenNodeIsNotWorker_ReturnsImmediately()
    {
        _configurationProvider.IsWorker.Returns(false);
        var subject = CreateSubject();

        await subject.Execute(new CancellationTokenSource());

        _configurationProvider.DidNotReceive().Validate();
        _executionContext.DidNotReceive().Initialize(Arg.Any<string>(), Arg.Any<IWorker>());
    }

    [Fact]
    public async Task Execute_WhenCancelledBeforeLoop_InitializesAndStops()
    {
        _configurationProvider.IsWorker.Returns(true);
        _configurationProvider.GetNodeName().Returns("worker-a");

        var subject = CreateSubject();
        var cts = new CancellationTokenSource();
        cts.Cancel();

        await subject.Execute(cts);

        _configurationProvider.Received(1).Validate();
        _executionContext.Received(1).Initialize("worker-a", subject);
        Assert.True(subject.IsStopped);
    }

    [Fact]
    public async Task Add_WhenTaskAlreadyExists_DoesNotStartAgain()
    {
        _configurationProvider.IsWorker.Returns(true);
        _configurationProvider.GetNodeName().Returns("worker-a");

        var connector = Substitute.For<IConnector>();
        var runningTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        connector.Execute(Arg.Any<string>(), Arg.Any<CancellationTokenSource>()).Returns(runningTcs.Task);

        var provider = Substitute.For<IServiceProvider>();
        provider.GetService(typeof(IConnector)).Returns(connector);

        var scope = Substitute.For<IServiceScope>();
        scope.ServiceProvider.Returns(provider);
        _scopeFactory.CreateScope().Returns(scope);

        var subject = CreateSubject();
        var cts = new CancellationTokenSource();
        cts.Cancel();
        await subject.Execute(cts);

        await subject.Add("orders");
        await subject.Add("orders");

        await connector.Received(1).Execute("orders", Arg.Any<CancellationTokenSource>());

        runningTcs.TrySetResult();
        await subject.Remove("orders");
    }

    [Fact]
    public async Task Refresh_Delete_WhenConnectorIsRunning_RemovesConnector()
    {
        _configurationProvider.IsWorker.Returns(true);
        _configurationProvider.GetNodeName().Returns("worker-a");

        var connector = Substitute.For<IConnector>();
        var runningTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        connector.Execute(Arg.Any<string>(), Arg.Any<CancellationTokenSource>()).Returns(runningTcs.Task);

        var provider = Substitute.For<IServiceProvider>();
        provider.GetService(typeof(IConnector)).Returns(connector);

        var scope = Substitute.For<IServiceScope>();
        scope.ServiceProvider.Returns(provider);
        _scopeFactory.CreateScope().Returns(scope);

        var subject = CreateSubject();
        var cts = new CancellationTokenSource();
        cts.Cancel();
        await subject.Execute(cts);

        await subject.Add("orders");
        Assert.True(subject.IsRunning("orders"));

        await subject.Refresh("orders", isDelete: true);

        _configurationProvider.Received().ReloadWorkerConfig();
        Assert.False(subject.IsRunning("orders"));

        runningTcs.TrySetResult();
    }

    [Fact]
    public async Task Add_WhenConnectorCannotBeResolved_DoesNotTrackConnector()
    {
        _configurationProvider.IsWorker.Returns(true);
        _configurationProvider.GetNodeName().Returns("worker-a");

        var provider = Substitute.For<IServiceProvider>();
        provider.GetService(typeof(IConnector)).Returns((object)null);

        var scope = Substitute.For<IServiceScope>();
        scope.ServiceProvider.Returns(provider);
        _scopeFactory.CreateScope().Returns(scope);

        var subject = CreateSubject();
        var cts = new CancellationTokenSource();
        cts.Cancel();
        await subject.Execute(cts);

        await subject.Add("missing-connector");

        Assert.False(subject.IsRunning("missing-connector"));
    }

    private Worker CreateSubject() => new(_logger, _scopeFactory, _executionContext, _configurationProvider);
}
