using System;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Providers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Connectors;

public class SinkTaskTests
{
    private readonly IConfigurationProvider _configurationProvider;
    private readonly IExecutionContext _executionContext;
    private readonly IConnectRecordCollection _sinkRecordCollection;
    private readonly SinkTask _sinkTask;

    public SinkTaskTests()
    {
        _configurationProvider = Substitute.For<IConfigurationProvider>();
        _executionContext = Substitute.For<IExecutionContext>();
        _sinkRecordCollection = Substitute.For<IConnectRecordCollection>();
        var logger = Substitute.For<ILogger<SinkTask>>();

        _configurationProvider
            .GetParallelRetryOptions(Arg.Any<string>())
            .Returns(new ParallelRetryOptions { Attempts = 3 });

        _sinkTask = new SinkTask(
            _configurationProvider, _executionContext, _sinkRecordCollection, logger);
    }

    [Fact]
    public async Task Execute_WhenSubscribeFails_SetsIsStoppedAndReturnsEarly()
    {
        _sinkRecordCollection.TrySubscribe().Returns(false);
        var cts = new CancellationTokenSource();

        await _sinkTask.Execute("connector", 1, cts);

        Assert.True(_sinkTask.IsStopped);
        _executionContext.Received(1).Initialize("connector", 1, _sinkTask);
        await _sinkRecordCollection.Received(1).Setup(ConnectorType.Sink, "connector", 1);
        _sinkRecordCollection.Received(1).TrySubscribe();
        await _sinkRecordCollection.Received(0).Consume(Arg.Any<CancellationToken>());
        _sinkRecordCollection.Received(0).Cleanup();
    }

    [Fact]
    public async Task Execute_WhenCancelledAtLoopStart_CleansUpWithoutBatch()
    {
        // Pre-cancel token to skip loop body entirely.
        _sinkRecordCollection.TrySubscribe().Returns(true);
        var cts = new CancellationTokenSource();
        cts.Cancel();

        await _sinkTask.Execute("connector", 1, cts);

        _sinkRecordCollection.Received(0).Clear();
        await _sinkRecordCollection.Received(0).Consume(Arg.Any<CancellationToken>());
        _sinkRecordCollection.Received(1).Cleanup();
        Assert.True(_sinkTask.IsStopped);
    }

    [Fact]
    public async Task Execute_WhenRunsOnce_CallsFullBatchPipeline()
    {
        // Cancel at end of first iteration to stop before a second pass.
        _sinkRecordCollection.TrySubscribe().Returns(true);
        var cts = new CancellationTokenSource();
        _sinkRecordCollection
            .When(x => x.NotifyEndOfPartition())
            .Do(_ => cts.Cancel());

        await _sinkTask.Execute("connector", 1, cts);

        _sinkRecordCollection.Received(2).Clear(); // before + after batch
        await _sinkRecordCollection.Received(1).Consume(Arg.Any<CancellationToken>());
        await _sinkRecordCollection.Received(1).Process();
        await _sinkRecordCollection.Received(1).Sink();
        _sinkRecordCollection.Received(1).Commit();
        _sinkRecordCollection.Received(1).Record();
        await _sinkRecordCollection.Received(1).NotifyEndOfPartition();
        _sinkRecordCollection.Received(1).Cleanup();
    }

    [Fact]
    public async Task Execute_WhenDeadLetterEnabled_CallsDeadLetterInFinally()
    {
        _sinkRecordCollection.TrySubscribe().Returns(true);
        _configurationProvider.IsDeadLetterEnabled(Arg.Any<string>()).Returns(true);
        var cts = new CancellationTokenSource();
        _sinkRecordCollection
            .When(x => x.NotifyEndOfPartition())
            .Do(_ => cts.Cancel());

        await _sinkTask.Execute("connector", 1, cts);

        await _sinkRecordCollection.Received(1).DeadLetter();
    }

    [Fact]
    public async Task Execute_WhenDeadLetterDisabled_SkipsDeadLetter()
    {
        _sinkRecordCollection.TrySubscribe().Returns(true);
        _configurationProvider.IsDeadLetterEnabled(Arg.Any<string>()).Returns(false);
        var cts = new CancellationTokenSource();
        _sinkRecordCollection
            .When(x => x.NotifyEndOfPartition())
            .Do(_ => cts.Cancel());

        await _sinkTask.Execute("connector", 1, cts);

        await _sinkRecordCollection.Received(0).DeadLetter();
    }

    [Fact]
    public async Task Execute_SetsIsStoppedAfterCompletion()
    {
        var cts = new CancellationTokenSource();

        await _sinkTask.Execute("connector", 1, cts);

        Assert.True(_sinkTask.IsStopped);
    }
}
