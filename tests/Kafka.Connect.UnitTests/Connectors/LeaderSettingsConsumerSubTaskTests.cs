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

public class LeaderSettingsConsumerSubTaskTests
{
    private readonly IExecutionContext _executionContext;
    private readonly IConfigurationProvider _configurationProvider;
    private readonly IConnectRecordCollection _leaderRecordCollection;
    private readonly LeaderSettingsConsumerSubTask _consumerSubTask;

    public LeaderSettingsConsumerSubTaskTests()
    {
        _executionContext = Substitute.For<IExecutionContext>();
        _configurationProvider = Substitute.For<IConfigurationProvider>();
        _leaderRecordCollection = Substitute.For<IConnectRecordCollection>();
        var logger = Substitute.For<ILogger<LeaderSettingsConsumerSubTask>>();

        _configurationProvider
            .GetParallelRetryOptions(Arg.Any<string>())
            .Returns(new ParallelRetryOptions { Attempts = 3 });

        _consumerSubTask = new LeaderSettingsConsumerSubTask(
            _executionContext, _configurationProvider, _leaderRecordCollection, logger);
    }

    [Fact]
    public async Task Execute_WhenSubscribeFails_SetsIsStoppedAndReturnsEarly()
    {
        _leaderRecordCollection.TrySubscribe().Returns(false);

        await _consumerSubTask.Execute("connector", 1, new CancellationTokenSource());

        Assert.True(_consumerSubTask.IsStopped);
        _executionContext.Received(1).Initialize("connector", 1, _consumerSubTask);
        await _leaderRecordCollection.Received(1).Setup(ConnectorType.Leader, "connector", 1);
        _leaderRecordCollection.Received(1).TrySubscribe();
        await _leaderRecordCollection.Received(0).Consume(Arg.Any<CancellationToken>());
        _leaderRecordCollection.Received(0).Cleanup();
    }

    [Fact]
    public async Task Execute_WhenPreCancelled_CleansUpWithoutProcessing()
    {
        _leaderRecordCollection.TrySubscribe().Returns(true);
        var cts = new CancellationTokenSource();
        cts.Cancel();

        await _consumerSubTask.Execute("connector", 1, cts);

        _leaderRecordCollection.Received(0).ClearAll();
        await _leaderRecordCollection.Received(0).Consume(Arg.Any<CancellationToken>());
        _leaderRecordCollection.Received(1).Cleanup();
        Assert.True(_consumerSubTask.IsStopped);
    }

    [Fact]
    public async Task Execute_WhenRunsOnce_CallsFullPipeline()
    {
        // Cancel after Process → 1 complete iteration
        _leaderRecordCollection.TrySubscribe().Returns(true);
        var cts = new CancellationTokenSource();
        _leaderRecordCollection
            .When(x => x.Process())
            .Do(_ => cts.Cancel());

        await _consumerSubTask.Execute("connector", 1, cts);

        _leaderRecordCollection.Received(1).ClearAll();
        _leaderRecordCollection.Received(1).Clear();
        await _leaderRecordCollection.Received(1).Consume(Arg.Any<CancellationToken>());
        await _leaderRecordCollection.Received(1).Process();
        await _leaderRecordCollection.Received(1).Store(true);
        _leaderRecordCollection.Received(1).Record();
        _leaderRecordCollection.Received(1).Cleanup();
    }

    [Fact]
    public async Task Execute_OnFirstIteration_StoresWithRefreshTrue()
    {
        _leaderRecordCollection.TrySubscribe().Returns(true);
        var cts = new CancellationTokenSource();
        _leaderRecordCollection
            .When(x => x.Store(Arg.Any<bool>()))
            .Do(_ => cts.Cancel());

        await _consumerSubTask.Execute("connector", 1, cts);

        await _leaderRecordCollection.Received(1).Store(true);
    }

    [Fact]
    public async Task Execute_SetsIsStoppedAfterCompletion()
    {
        var cts = new CancellationTokenSource();

        await _consumerSubTask.Execute("connector", 1, cts);

        Assert.True(_consumerSubTask.IsStopped);
    }
}
