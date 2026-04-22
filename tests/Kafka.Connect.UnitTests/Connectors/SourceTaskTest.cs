using System;
using System.Collections.Generic;
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

public class SourceTaskTests
{
    private readonly IExecutionContext _executionContext;
    private readonly IConfigurationProvider _configurationProvider;
    private readonly IConnectRecordCollection _pollRecordCollection;
    private readonly SourceTask _sourceTask;

    public SourceTaskTests()
    {
        _executionContext = Substitute.For<IExecutionContext>();
        _configurationProvider = Substitute.For<IConfigurationProvider>();
        _pollRecordCollection = Substitute.For<IConnectRecordCollection>();
        var logger = Substitute.For<ILogger<SourceTask>>();

        _configurationProvider
            .GetBatchConfig(Arg.Any<string>())
            .Returns(new BatchConfig());
        _configurationProvider
            .GetParallelRetryOptions(Arg.Any<string>())
            .Returns(new ParallelRetryOptions { Attempts = 3, DegreeOfParallelism = 1 });

        _sourceTask = new SourceTask(
            _executionContext, _configurationProvider, _pollRecordCollection, logger);
    }

    [Fact]
    public async Task Execute_WhenSubscribeFails_SetsIsStoppedAndReturnsEarly()
    {
        const string connector = "test-connector";
        const int taskId = 1;
        _pollRecordCollection.TrySubscribe().Returns(false);

        await _sourceTask.Execute(connector, taskId, new CancellationTokenSource());

        Assert.True(_sourceTask.IsStopped);
        Received.InOrder(() =>
        {
            _executionContext.Initialize(connector, taskId, _sourceTask);
            _pollRecordCollection.Setup(ConnectorType.Source, connector, taskId);
            _pollRecordCollection.TrySubscribe();
        });
        await _pollRecordCollection.Received(0).Consume(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Execute_WhenPublisherFails_SetsIsStoppedAndReturnsEarly()
    {
        const string connector = "test-connector";
        const int taskId = 1;
        _pollRecordCollection.TrySubscribe().Returns(true);
        _pollRecordCollection.TryPublisher().Returns(false);

        await _sourceTask.Execute(connector, taskId, new CancellationTokenSource());

        Assert.True(_sourceTask.IsStopped);
        Received.InOrder(() =>
        {
            _executionContext.Initialize(connector, taskId, _sourceTask);
            _pollRecordCollection.Setup(ConnectorType.Source, connector, taskId);
            _pollRecordCollection.TrySubscribe();
            _pollRecordCollection.TryPublisher();
        });
        await _pollRecordCollection.Received(0).Consume(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Execute_WhenCancelledAtLoopStart_CleansUpWithoutBatch()
    {
        // Pre-cancel token to skip loop body entirely.
        const string connector = "test-connector";
        const int taskId = 1;
        _pollRecordCollection.TrySubscribe().Returns(true);
        _pollRecordCollection.TryPublisher().Returns(true);
        var cts = new CancellationTokenSource();
        cts.Cancel();

        await _sourceTask.Execute(connector, taskId, cts);

        _executionContext.Received(1).Initialize(connector, taskId, _sourceTask);
        await _pollRecordCollection.Received(0).Consume(Arg.Any<CancellationToken>());
        Assert.True(_sourceTask.IsStopped);
    }

    [Fact]
    public async Task Execute_WhenRunsOnce_CallsConsumeAndCommit()
    {
        // Cancel after Purge at end of first loop to avoid a second iteration.
        const string connector = "test-connector";
        const int taskId = 1;
        _pollRecordCollection.TrySubscribe().Returns(true);
        _pollRecordCollection.TryPublisher().Returns(true);
        _pollRecordCollection.GetCommands().Returns((IList<CommandRecord>)new List<CommandRecord>());
        var cts = new CancellationTokenSource();
        _pollRecordCollection
            .When(x => x.Purge(ConnectorType.Source, connector, taskId))
            .Do(_ => cts.Cancel());

        await _sourceTask.Execute(connector, taskId, cts);

        await _pollRecordCollection.Received(1).Consume(Arg.Any<CancellationToken>());
        await _pollRecordCollection.Received(1).GetCommands();
        _pollRecordCollection.Received(1).Commit(Arg.Any<IList<CommandRecord>>());
        _pollRecordCollection.Received(1).Clear();
        await _pollRecordCollection.Received(1).Purge(ConnectorType.Source, connector, taskId);
    }
}