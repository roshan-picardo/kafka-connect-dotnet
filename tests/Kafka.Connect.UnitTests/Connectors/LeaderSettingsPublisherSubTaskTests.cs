using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Kafka.Connect.Configurations;
using Kafka.Connect.Connectors;
using Kafka.Connect.Handlers;
using Kafka.Connect.Plugin.Logging;
using Kafka.Connect.Providers;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Connectors;

public class LeaderSettingsPublisherSubTaskTests
{
    private readonly IExecutionContext _executionContext;
    private readonly IConnectRecordCollection _leaderRecordCollection;
    private readonly Channel<(string Connector, JsonObject Settings)> _channel;
    private readonly LeaderSettingsPublisherSubTask _publisherSubTask;

    public LeaderSettingsPublisherSubTaskTests()
    {
        _executionContext = Substitute.For<IExecutionContext>();
        _leaderRecordCollection = Substitute.For<IConnectRecordCollection>();
        var logger = Substitute.For<ILogger<LeaderSettingsPublisherSubTask>>();

        _channel = Channel.CreateUnbounded<(string, JsonObject)>();
        _executionContext.ConfigurationChannel.Returns(_channel);

        _publisherSubTask = new LeaderSettingsPublisherSubTask(
            _executionContext, _leaderRecordCollection, logger);
    }

    [Fact]
    public async Task Execute_WhenPublisherFails_SetsIsStoppedAndReturnsEarly()
    {
        _leaderRecordCollection.TryPublisher().Returns(false);

        await _publisherSubTask.Execute("connector", 1, new CancellationTokenSource());

        Assert.True(_publisherSubTask.IsStopped);
        _executionContext.Received(1).Initialize("connector", 1, _publisherSubTask);
        await _leaderRecordCollection.Received(1).Setup(ConnectorType.Leader, "connector", 1);
        _leaderRecordCollection.Received(1).TryPublisher();
        _leaderRecordCollection.Received(0).Configure(Arg.Any<string>(), Arg.Any<(string, JsonObject)>());
        _leaderRecordCollection.Received(0).Cleanup();
    }

    [Fact]
    public async Task Execute_WhenPreCancelled_ExitsCleanly()
    {
        // Pre-cancelled token → ReadAsync throws OperationCanceledException → breaks → Cleanup
        _leaderRecordCollection.TryPublisher().Returns(true);
        var cts = new CancellationTokenSource();
        cts.Cancel();

        await _publisherSubTask.Execute("connector", 1, cts);

        _leaderRecordCollection.Received(0).Configure(Arg.Any<string>(), Arg.Any<(string, JsonObject)>());
        _leaderRecordCollection.Received(1).Cleanup();
        Assert.True(_publisherSubTask.IsStopped);
    }

    [Fact]
    public async Task Execute_WhenConfigurationPublished_ProcessesAndProduces()
    {
        // Write one item to the channel; cancel after Produce to exit cleanly
        _leaderRecordCollection.TryPublisher().Returns(true);
        var settings = new JsonObject();
        await _channel.Writer.WriteAsync(("connector", settings));
        var cts = new CancellationTokenSource();
        _leaderRecordCollection
            .When(x => x.Produce(Arg.Any<string>()))
            .Do(_ => cts.Cancel());

        await _publisherSubTask.Execute("connector", 1, cts);

        _leaderRecordCollection.Received(1).Configure("connector", Arg.Any<(string, JsonObject)>());
        await _leaderRecordCollection.Received(1).Process("connector");
        await _leaderRecordCollection.Received(1).Produce("connector");
        _leaderRecordCollection.Received(1).Record("connector");
        _leaderRecordCollection.Received(1).Cleanup();
    }

    [Fact]
    public async Task Execute_UpdatesLeaderAssignmentsBeforeLoop()
    {
        _leaderRecordCollection.TryPublisher().Returns(true);
        var cts = new CancellationTokenSource();
        cts.Cancel();

        await _publisherSubTask.Execute("connector", 2, cts);

        _executionContext.Received(1).UpdateLeaderAssignments("connector", 2, TopicType.Config);
    }

    [Fact]
    public async Task Execute_SetsIsStoppedAfterCompletion()
    {
        _leaderRecordCollection.TryPublisher().Returns(true);
        var cts = new CancellationTokenSource();
        cts.Cancel();

        await _publisherSubTask.Execute("connector", 1, cts);

        Assert.True(_publisherSubTask.IsStopped);
    }
}
