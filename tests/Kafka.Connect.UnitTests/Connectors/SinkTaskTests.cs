using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Plugin.Tokens;
using Kafka.Connect.Providers;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;

namespace UnitTests.Kafka.Connect.Connectors;

public class SinkTaskTests
{
    private readonly ISinkExceptionHandler _sinkExceptionHandler;
    private readonly IConfigurationProvider _configurationProvider;
    private readonly IExecutionContext _executionContext;
    private readonly IConnectRecordCollection _sinkRecordCollection;
    private readonly ITokenHandler _tokenHandler;
    private readonly SinkTask _sinkTask;

    public SinkTaskTests()
    {
        _sinkExceptionHandler = Substitute.For<ISinkExceptionHandler>();
        _configurationProvider = Substitute.For<IConfigurationProvider>();
        _executionContext = Substitute.For<IExecutionContext>();
        _sinkRecordCollection = Substitute.For<IConnectRecordCollection>();
        _tokenHandler = Substitute.For<ITokenHandler>();

        _sinkTask = new SinkTask(
            _sinkExceptionHandler,
            _configurationProvider,
            _executionContext,
            _sinkRecordCollection,
            _tokenHandler);
    }

    [Fact]
    public async Task Execute_ShouldSetupSinkRecordCollection()
    {
        // Arrange
        const string connector = "test-connector";
        const int taskId = 1;
        var cts = new CancellationTokenSource();

        // Act
        await _sinkTask.Execute(connector, taskId, cts);

        // Assert
        _executionContext.Received(1).Initialize(connector, taskId, _sinkTask);
        await _sinkRecordCollection.Received(1).Setup(ConnectorType.Sink, connector, taskId);
        _sinkRecordCollection.Received(1).TrySubscribe();
        _sinkRecordCollection.Received(0).Clear();
        await _sinkRecordCollection.Received(0).Consume(cts.Token);
        await _sinkRecordCollection.Received(0).Process();
        await _sinkRecordCollection.Received(0).Sink();
        _sinkRecordCollection.Received(0).Commit();
        _configurationProvider.Received(0).IsErrorTolerated(connector);
        _sinkRecordCollection.Received(0).Commit();
        _sinkExceptionHandler.Received(0).Handle(Arg.Any<Exception>(), Arg.Any<Action>());
        _sinkRecordCollection.Received(0).Record();
        await _sinkRecordCollection.Received(0).NotifyEndOfPartition();
        _sinkRecordCollection.Received(0).Cleanup();
    }

    [Fact]
    public async Task Execute_ShouldNotContinueIfSubscribeFails()
    {
        // Arrange
        const string connector = "test-connector";
        const int taskId = 1;
        var cts = new CancellationTokenSource();
        _sinkRecordCollection.TrySubscribe().Returns(false);

        // Act
        await _sinkTask.Execute(connector, taskId, cts);

        // Assert
        Assert.True(_sinkTask.IsStopped);
        _executionContext.Received(1).Initialize(connector, taskId, _sinkTask);
        await _sinkRecordCollection.Received(1).Setup(ConnectorType.Sink, connector, taskId);
        _sinkRecordCollection.Received(1).TrySubscribe();
        _sinkRecordCollection.Received(0).Clear();
        await _sinkRecordCollection.Received(0).Consume(cts.Token);
        await _sinkRecordCollection.Received(0).Process();
        await _sinkRecordCollection.Received(0).Sink();
        _sinkRecordCollection.Received(0).Commit();
        _configurationProvider.Received(0).IsErrorTolerated(connector);
        _sinkRecordCollection.Received(0).Commit();
        _sinkExceptionHandler.Received(0).Handle(Arg.Any<Exception>(), Arg.Any<Action>());
        _sinkRecordCollection.Received(0).Record();
        await _sinkRecordCollection.Received(0).NotifyEndOfPartition();
        _sinkRecordCollection.Received(0).Cleanup();
    }

    [Fact]
    public async Task Execute_ShouldConsumeProcessAndSinkRecords()
    {
        // Arrange
        const string connector = "test-connector";
        const int taskId = 1;
        var cts = new CancellationTokenSource();
        _sinkRecordCollection.TrySubscribe().Returns(true);
        var counter = 1;
        _tokenHandler.When(x => x.NoOp()).Do(_ =>
        {
            if (counter-- == 0) cts.Cancel();
        });

        // Act
        await _sinkTask.Execute(connector, taskId, cts);

        // Assert
        _executionContext.Received(1).Initialize(connector, taskId, _sinkTask);
        await _sinkRecordCollection.Received(1).Setup(ConnectorType.Sink, connector, taskId);
        _sinkRecordCollection.Received(1).TrySubscribe();
        _sinkRecordCollection.Received(1).Clear();
        await _sinkRecordCollection.Received(1).Consume(cts.Token);
        await _sinkRecordCollection.Received(1).Process();
        await _sinkRecordCollection.Received(1).Sink();
        _sinkRecordCollection.Received(1).Commit();
        _configurationProvider.Received(0).IsErrorTolerated(connector);
        _sinkExceptionHandler.Received(0).Handle(Arg.Any<Exception>(), Arg.Any<Action>());
        _sinkRecordCollection.Received(1).Record();
        await _sinkRecordCollection.Received(1).NotifyEndOfPartition();
        _sinkRecordCollection.Received(1).Cleanup();
    }

    [Fact]
    public async Task Execute_ShouldHandleExceptionIfErrorTolerated()
    {
        // Arrange
        const string connector = "test-connector";
        const int taskId = 1;
        var cts = new CancellationTokenSource();
        var exception = new Exception("Test exception");
        _configurationProvider.IsErrorTolerated(Arg.Any<string>()).Returns(true);
        _sinkRecordCollection.Consume(cts.Token).Throws(exception);
        _sinkRecordCollection.TrySubscribe().Returns(true);
        var counter = 1;
        _tokenHandler.When(x => x.NoOp()).Do(_ =>
        {
            if (counter-- == 0) cts.Cancel();
        });

        // Act
        await _sinkTask.Execute(connector, taskId, cts);

        // Assert
        _executionContext.Received(1).Initialize(connector, taskId, _sinkTask);
        await _sinkRecordCollection.Received(1).Setup(ConnectorType.Sink, connector, taskId);
        _sinkRecordCollection.Received(1).TrySubscribe();
        _sinkRecordCollection.Received(1).Clear();
        await _sinkRecordCollection.Received(1).Consume(cts.Token);
        await _sinkRecordCollection.Received(0).Process();
        await _sinkRecordCollection.Received(0).Sink();
        _sinkRecordCollection.Received(1).Commit();
        _configurationProvider.Received(1).IsErrorTolerated(connector);
        _sinkExceptionHandler.Received(1).Handle(Arg.Any<Exception>(), Arg.Any<Action>());
        _sinkRecordCollection.Received(1).Record();
        await _sinkRecordCollection.Received(1).NotifyEndOfPartition();
        _sinkRecordCollection.Received(1).Cleanup();
        await _sinkRecordCollection.Received(1).DeadLetter();
    }
    
    [Fact]
    public async Task Execute_ShouldHandleExceptionIfErrorToleratedContinueTask()
    {
        // Arrange
        const string connector = "test-connector";
        const int taskId = 1;
        var cts = new CancellationTokenSource();
        var exception = new Exception("Test exception");
        _configurationProvider.IsErrorTolerated(Arg.Any<string>()).Returns(true);
        _sinkRecordCollection.Consume(cts.Token).Throws(exception);
        _sinkRecordCollection.TrySubscribe().Returns(true);
        _sinkExceptionHandler.Handle(Arg.Any<Exception>(), Arg.Invoke());
        var counter = 1;
        _tokenHandler.When(x => x.NoOp()).Do(_ =>
        {
            if (counter-- == 0) cts.Cancel();
        });

        // Act
        await _sinkTask.Execute(connector, taskId, cts);

        // Assert
        _executionContext.Received(1).Initialize(connector, taskId, _sinkTask);
        await _sinkRecordCollection.Received(1).Setup(ConnectorType.Sink, connector, taskId);
        _sinkRecordCollection.Received(1).TrySubscribe();
        _sinkRecordCollection.Received(1).Clear();
        await _sinkRecordCollection.Received(1).Consume(cts.Token);
        await _sinkRecordCollection.Received(0).Process();
        await _sinkRecordCollection.Received(0).Sink();
        _sinkRecordCollection.Received(1).Commit();
        _configurationProvider.Received(2).IsErrorTolerated(connector);
        _sinkExceptionHandler.Received(1).Handle(Arg.Any<Exception>(), Arg.Any<Action>());
        _sinkRecordCollection.Received(1).Record();
        await _sinkRecordCollection.Received(1).NotifyEndOfPartition();
        _sinkRecordCollection.Received(1).Cleanup();
        await _sinkRecordCollection.Received(1).DeadLetter();
    }
    
    [Fact]
    public async Task Execute_ShouldHandleExceptionIfErrorNotToleratedCancelTask()
    {
        // Arrange
        const string connector = "test-connector";
        const int taskId = 1;
        var cts = new CancellationTokenSource();
        var exception = new Exception("Test exception");
        _configurationProvider.IsErrorTolerated(Arg.Any<string>()).Returns(false);
        _sinkRecordCollection.Consume(cts.Token).Throws(exception);
        _sinkRecordCollection.TrySubscribe().Returns(true);
        _sinkExceptionHandler.Handle(Arg.Any<Exception>(), Arg.Invoke());

        // Act
        await _sinkTask.Execute(connector, taskId, cts);

        // Assert
        _executionContext.Received(1).Initialize(connector, taskId, _sinkTask);
        await _sinkRecordCollection.Received(1).Setup(ConnectorType.Sink, connector, taskId);
        _sinkRecordCollection.Received(1).TrySubscribe();
        _sinkRecordCollection.Received(1).Clear();
        await _sinkRecordCollection.Received(1).Consume(cts.Token);
        await _sinkRecordCollection.Received(0).Process();
        await _sinkRecordCollection.Received(0).Sink();
        _sinkRecordCollection.Received(0).Commit();
        _configurationProvider.Received(2).IsErrorTolerated(connector);
        _sinkExceptionHandler.Received(1).Handle(Arg.Any<Exception>(), Arg.Any<Action>());
        _sinkRecordCollection.Received(1).Record();
        await _sinkRecordCollection.Received(1).NotifyEndOfPartition();
        _sinkRecordCollection.Received(1).Cleanup();
        await _sinkRecordCollection.Received(0).DeadLetter();
    }

    [Fact]
    public async Task Execute_ShouldCancelExecutionIfErrorNotTolerated()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        _configurationProvider.IsErrorTolerated(Arg.Any<string>()).Returns(false);
        _sinkRecordCollection.TrySubscribe().Returns(true);
        var counter = 1;
        _tokenHandler.When(x => x.NoOp()).Do(_ =>
        {
            if (counter-- == 0) cts.Cancel();
        });

        // Act
        await _sinkTask.Execute("test-connector", 1, cts);

        // Assert
        Assert.True(cts.IsCancellationRequested);
    }

    [Fact]
    public async Task Execute_ShouldCleanupAfterExecution()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        _sinkRecordCollection.TrySubscribe().Returns(true);
        var counter = 1;
        _tokenHandler.When(x => x.NoOp()).Do(_ =>
        {
            if (counter-- == 0) cts.Cancel();
        });

        // Act
        await _sinkTask.Execute("test-connector", 1, cts);

        // Assert
        _sinkRecordCollection.Received(1).Cleanup();
    }

    [Fact]
    public async Task Execute_ShouldSetIsStoppedToTrueAfterExecution()
    {
        // Arrange
        var cts = new CancellationTokenSource();

        // Act
        await _sinkTask.Execute("test-connector", 1, cts);

        // Assert
        Assert.True(_sinkTask.IsStopped);
    }

    IEnumerable<ConnectRecord> StopBy(List<ConnectRecord> records)
    {
        return records.TakeWhile(record => record.Status != Status.Failed);
    }
    
   
}
