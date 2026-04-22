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

public class WorkerTaskTests
{
    private readonly IConfigurationProvider _configurationProvider;
    private readonly IExecutionContext _executionContext;
    private readonly IConnectRecordCollection _workerRecordCollection;
    private readonly WorkerTask _workerTask;

    public WorkerTaskTests()
    {
        _configurationProvider = Substitute.For<IConfigurationProvider>();
        _executionContext = Substitute.For<IExecutionContext>();
        _workerRecordCollection = Substitute.For<IConnectRecordCollection>();
        var logger = Substitute.For<ILogger<WorkerTask>>();

        _configurationProvider
            .GetParallelRetryOptions(Arg.Any<string>())
            .Returns(new ParallelRetryOptions { Attempts = 3 });

        _workerTask = new WorkerTask(
            _configurationProvider, _executionContext, _workerRecordCollection, logger);
    }

    [Fact]
    public async Task Execute_WhenSubscribeFails_SetsIsStoppedAndReturnsEarly()
    {
        _workerRecordCollection.TrySubscribe().Returns(false);

        await _workerTask.Execute("connector", 1, new CancellationTokenSource());

        Assert.True(_workerTask.IsStopped);
        _executionContext.Received(1).Initialize("connector", 1, _workerTask);
        await _workerRecordCollection.Received(1).Setup(ConnectorType.Worker, "connector", 1);
        _workerRecordCollection.Received(1).TrySubscribe();
        await _workerRecordCollection.Received(0).Consume(Arg.Any<CancellationToken>());
        _workerRecordCollection.Received(0).Cleanup();
    }

    [Fact]
    public async Task Execute_WhenPreCancelled_CleansUpWithoutProcessing()
    {
        _workerRecordCollection.TrySubscribe().Returns(true);
        var cts = new CancellationTokenSource();
        cts.Cancel();

        await _workerTask.Execute("connector", 1, cts);

        _workerRecordCollection.Received(0).ClearAll();
        await _workerRecordCollection.Received(0).Consume(Arg.Any<CancellationToken>());
        _workerRecordCollection.Received(1).Cleanup();
        Assert.True(_workerTask.IsStopped);
    }

    [Fact]
    public async Task Execute_WhenRunsOnce_CallsConsumeAndRecord()
    {
        // Cancel after Consume → 1 complete loop iteration with 0 records
        _workerRecordCollection.TrySubscribe().Returns(true);
        var cts = new CancellationTokenSource();
        _workerRecordCollection
            .When(x => x.Consume(Arg.Any<CancellationToken>()))
            .Do(_ => cts.Cancel());

        await _workerTask.Execute("connector", 1, cts);

        _workerRecordCollection.Received(1).ClearAll();
        _workerRecordCollection.Received(1).Clear();
        await _workerRecordCollection.Received(1).Consume(Arg.Any<CancellationToken>());
        // Count() = 0 by default → Process/Store/Refresh not called
        await _workerRecordCollection.Received(0).Process();
        _workerRecordCollection.Received(1).Record();
        _workerRecordCollection.Received(1).Record("connector");
        _workerRecordCollection.Received(1).Cleanup();
    }

    [Fact]
    public async Task Execute_WhenRecordsConsumed_ProcessesAndStores()
    {
        // Count() > 0 → Process, Store, Refresh are called
        _workerRecordCollection.TrySubscribe().Returns(true);
        _workerRecordCollection.Count().Returns(5);
        var cts = new CancellationTokenSource();
        _workerRecordCollection
            .When(x => x.Consume(Arg.Any<CancellationToken>()))
            .Do(_ => cts.Cancel());

        await _workerTask.Execute("connector", 1, cts);

        await _workerRecordCollection.Received(1).Process();
        await _workerRecordCollection.Received(1).Store(Arg.Any<string>());
        await _workerRecordCollection.Received(1).Refresh(Arg.Any<string>());
    }

    [Fact]
    public async Task Execute_SetsIsStoppedAfterCompletion()
    {
        var cts = new CancellationTokenSource();

        await _workerTask.Execute("connector", 1, cts);

        Assert.True(_workerTask.IsStopped);
    }
}
