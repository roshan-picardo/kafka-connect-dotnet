using System;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Tokens;
using Xunit;

namespace UnitTests.Kafka.Connect.Tokens;

public class PauseTokenSourceTests
{
    [Fact]
    public void New_CanStartPausedOrResumed()
    {
        Assert.True(PauseTokenSource.New(paused: true).IsPaused);
        Assert.False(PauseTokenSource.New(paused: false).IsPaused);
    }

    [Fact]
    public async Task WaitWhilePaused_WhenNotPaused_CompletesImmediately()
    {
        var source = PauseTokenSource.New();

        await source.WaitWhilePaused(CancellationToken.None);

        Assert.False(source.IsPaused);
    }

    [Fact]
    public async Task WaitWhilePaused_WhenPaused_ResumesOnCancellation()
    {
        var source = PauseTokenSource.New(paused: true);
        using var cts = new CancellationTokenSource();

        var waiting = source.WaitWhilePaused(cts.Token);
        cts.Cancel();

        await waiting.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.False(source.IsPaused);
    }

    [Fact]
    public async Task WaitUntilTimeout_WithZeroTimeout_ResumesImmediately()
    {
        var source = PauseTokenSource.New(paused: true);

        await source.WaitUntilTimeout(0, CancellationToken.None);

        await AssertEventuallyAsync(() => !source.IsPaused, TimeSpan.FromSeconds(2));
    }

    [Fact]
    public async Task WaitUntilTimeout_WithPositiveTimeout_PausesCancelsAndAutoResumes()
    {
        var source = PauseTokenSource.New();
        var paused = false;
        var resumed = false;

        source.ConfigureOnPaused(() => paused = true)
            .ConfigureOnResumed(() => resumed = true);

        using var linked = new CancellationTokenSource();
        source.AddLinkedTokenSource(linked);

        await source.WaitUntilTimeout(40, CancellationToken.None);

        Assert.True(paused);
        Assert.True(linked.IsCancellationRequested);
        Assert.True(source.IsPaused);

        await Task.Delay(120);

        Assert.True(resumed);
        Assert.False(source.IsPaused);
    }

    [Fact]
    public async Task WaitUntilTimeout_WhenAlreadyPaused_DoesNotDeadlockAndResumes()
    {
        var source = PauseTokenSource.New(paused: true);
        using var cts = new CancellationTokenSource(100);

        await source.WaitUntilTimeout(0, cts.Token);

        await AssertEventuallyAsync(() => !source.IsPaused, TimeSpan.FromSeconds(2));
    }

    private static async Task AssertEventuallyAsync(Func<bool> condition, TimeSpan timeout)
    {
        var start = DateTime.UtcNow;
        while (DateTime.UtcNow - start < timeout)
        {
            if (condition())
            {
                return;
            }

            await Task.Delay(10);
        }

        Assert.True(condition());
    }
}
