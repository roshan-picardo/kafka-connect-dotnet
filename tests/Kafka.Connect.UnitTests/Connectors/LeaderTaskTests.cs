using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Connectors;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Connectors;

public class LeaderTaskTests
{
    private readonly ILeaderSubTask _subTask1;
    private readonly ILeaderSubTask _subTask2;

    public LeaderTaskTests()
    {
        _subTask1 = Substitute.For<ILeaderSubTask>();
        _subTask2 = Substitute.For<ILeaderSubTask>();
    }

    [Fact]
    public async Task Execute_WhenTaskId1_InvokesFirstSubTask()
    {
        var leaderTask = new LeaderTask(new[] { _subTask1, _subTask2 });
        var cts = new CancellationTokenSource();

        await leaderTask.Execute("connector", 1, cts);

        await _subTask1.Received(1).Execute("connector", 1, cts);
        await _subTask2.Received(0).Execute(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationTokenSource>());
    }

    [Fact]
    public async Task Execute_WhenTaskId2_InvokesSecondSubTask()
    {
        var leaderTask = new LeaderTask(new[] { _subTask1, _subTask2 });
        var cts = new CancellationTokenSource();

        await leaderTask.Execute("connector", 2, cts);

        await _subTask2.Received(1).Execute("connector", 2, cts);
        await _subTask1.Received(0).Execute(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationTokenSource>());
    }

    [Fact]
    public async Task Execute_WhenTaskIdOutOfRange_NoSubTaskInvoked()
    {
        var leaderTask = new LeaderTask(new[] { _subTask1 });
        var cts = new CancellationTokenSource();

        await leaderTask.Execute("connector", 5, cts);

        await _subTask1.Received(0).Execute(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationTokenSource>());
    }

    [Fact]
    public async Task Execute_SetsIsStoppedAfterExecution()
    {
        var leaderTask = new LeaderTask(new[] { _subTask1 });

        await leaderTask.Execute("connector", 1, new CancellationTokenSource());

        Assert.True(leaderTask.IsStopped);
    }

    [Fact]
    public async Task Execute_WhenEmptySubTaskList_SetsIsStoppedWithoutException()
    {
        var leaderTask = new LeaderTask(new List<ILeaderSubTask>());

        await leaderTask.Execute("connector", 1, new CancellationTokenSource());

        Assert.True(leaderTask.IsStopped);
        Assert.False(leaderTask.IsPaused);
    }
}
